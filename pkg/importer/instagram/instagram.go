/*
Copyright 2018 The Perkeep Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package instagram provides an importer for instagram.com using an unofficial API
package instagram // import "perkeep.org/pkg/importer/instagram"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"perkeep.org/internal/httputil"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/importer"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/schema/nodeattr"
	"perkeep.org/pkg/search"

	"github.com/ahmdrz/goinsta/v2"
	"go4.org/ctxutil"
	"go4.org/syncutil"
)

const (
	// runCompleteVersion is a cache-busting version number of the
	// importer code. It should be incremented whenever the
	// behavior of this importer is updated enough to warrant a
	// complete run.  Otherwise, if the importer runs to
	// completion, this version number is recorded on the account
	// permanode and subsequent importers can stop early.
	runCompleteVersion = "0"

	// Import types
	nodeTypeCarousel = "instagram.com:carousel" // album

	// Import attributes
	// attrInstagramID is the Instagram API object ID of the post
	attrInstagramID = "instagram.com:id"
	// attrCarouselItemCount is the number of items in a carousel post
	attrCarouselItemCount = "instagram.com:carouselItemCount"

	importAtOnce = 5 // number of concurrent imports
)

var (
	logger = log.New(os.Stderr, "instagram: ", log.LstdFlags)
	logf   = logger.Printf
)

func init() {
	importer.Register("instagram", &imp{})
}

var _ importer.ImporterSetupHTMLer = (*imp)(nil)

type imp struct {
	importer.OAuth1 // for CallbackRequestAccount and CallbackURLParameters
}

func (*imp) Properties() importer.Properties {
	return importer.Properties{
		Title:               "Instagram",
		Description:         "import your instagram posts",
		SupportsIncremental: true,
		NeedsAPIKey:         true,

		MemoryOnlyClientSecret: true,
	}
}

func (*imp) IsAccountReady(acctNode *importer.Object) (bool, error) {
	if acctNode.Attr(importer.AcctAttrUserName) != "" && acctNode.Attr(importer.AcctAttrUserID) != "" {
		return true, nil
	}
	return false, nil
}

func (im *imp) SummarizeAccount(acct *importer.Object) string {
	if ok, _ := im.IsAccountReady(acct); !ok {
		return "Not configured"
	}
	s := fmt.Sprintf("@%s (%s), instagram id %s",
		acct.Attr(importer.AcctAttrUserName),
		acct.Attr(importer.AcctAttrName),
		acct.Attr(importer.AcctAttrUserID),
	)
	return s
}

func (im *imp) AccountSetupHTML(host *importer.Host) string {
	return `
<h1>Configuring Instagram</h1>
<p>
Fill your Instagram account credentials into the "Client ID" (username)
and "Client Secret" (password) boxes above.
</p>
<p>
Note that setting up the importer repeatedly (e.g. when restarting your instance)
can lead to your IP being blacklisted by Instagram.
</p>
`
}

func (im imp) ServeSetup(w http.ResponseWriter, r *http.Request, ctx *importer.SetupContext) error {
	username, password, err := ctx.Credentials()
	if err != nil {
		err := fmt.Errorf("error getting credentials: %v", err)
		httputil.ServeError(w, r, err)
		return err
	}
	if username == "" || password == "" {
		err := fmt.Errorf("Username and password are required")
		httputil.ServeError(w, r, err)
		return err
	}

	// TODO(robertgzr):
	// Every login done by goinsta uses a new UUID and PhoneID, which could lead to the IP
	// of the instance being blacklisted by Instagram, see:
	// https://github.com/tducasse/go-instabot/issues/1
	// https://github.com/liamcottle/Instagram-SDK-PHP/issues/32
	//
	// The solution for this would be to store and reuse the session cookies, using:
	// https://github.com/ahmdrz/goinsta/commit/4eb2917e24f734a4792d9f94a862841265c3b62a
	insta := goinsta.New(username, password)
	if err := insta.Login(); err != nil {
		err := fmt.Errorf("error on login: %v", err)
		httputil.ServeError(w, r, err)
		return err
	}
	defer func() {
		if err := insta.Logout(); err != nil {
			logf("Logout error: %s", err)
		}
	}()

	acc, err := insta.Profiles.ByName(username)
	if err != nil {
		err := fmt.Errorf("error syncing account: %v", err)
		httputil.ServeError(w, r, err)
		return err
	}
	if err := ctx.AccountNode.SetAttrs(
		importer.AcctAttrUserID, strconv.FormatInt(acc.ID, 10),
		importer.AcctAttrName, acc.FullName,
		importer.AcctAttrUserName, acc.Username,
	); err != nil {
		err := fmt.Errorf("Error setting attribute: %v", err)
		httputil.ServeError(w, r, err)
		return err
	}

	http.Redirect(w, r, ctx.CallbackURL(), http.StatusFound)
	return nil
}

func (im imp) ServeCallback(w http.ResponseWriter, r *http.Request, ctx *importer.SetupContext) {
	http.Redirect(w, r, ctx.AccountURL(), http.StatusFound)
	return
}

type run struct {
	*importer.RunContext
	im          *imp
	incremental bool
	username    string
}

var forceFullImport, _ = strconv.ParseBool(os.Getenv("PERKEEP_INSTAGRAM_FULL_IMPORT"))

func (im *imp) Run(rctx *importer.RunContext) error {
	logf("Running importer.")

	username, password, err := rctx.Credentials()
	if err != nil {
		return err
	}

	insta := goinsta.New(
		username,
		password,
	)
	if err := insta.Login(); err != nil {
		logf("Login error: %s", err)
		return err
	}
	defer func() {
		if err := insta.Logout(); err != nil {
			logf("Logout error: %s", err)
		}
	}()

	r := &run{
		RunContext:  rctx,
		im:          im,
		incremental: !forceFullImport && rctx.AccountNode().Attr(importer.AcctAttrCompletedVersion) == runCompleteVersion,
		username:    username,
	}

	acc, err := insta.Profiles.ByName(r.username)
	if err != nil {
		return err
	}

	if err := r.importPosts(acc.Feed()); err != nil {
		return err
	}

	if err := rctx.AccountNode().SetAttrs(importer.AcctAttrCompletedVersion, runCompleteVersion); err != nil {
		return err
	}

	return nil
}

func (r *run) importPosts(feed *goinsta.FeedMedia) error {
	root := r.RootNode()

	nodeTitle := fmt.Sprintf("Instagram posts for @%s", r.username)
	if err := root.SetAttr(nodeattr.Title, nodeTitle); err != nil {
		return err
	}

	logf("Beginning posts import for @%s", r.username)

	for feed.Next() {
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		default:
		}

		if feed.NumResults == 0 {
			logf("Feed for @%s was empty, stopping...", r.username)
			return nil
		}

		gate := syncutil.NewGate(importAtOnce)
		var grp syncutil.Group
		var anyNewMu sync.Mutex
		var anyNew = false

		for i := range feed.Items {
			p := feed.Items[i]

			gate.Start()
			grp.Go(func() error {
				defer gate.Done()
				_, alreadyHad, err := r.importPost(root, p)
				if err != nil {
					return fmt.Errorf("error importing post %s: %v", p.ID, err)
				}

				if !alreadyHad {
					anyNewMu.Lock()
					anyNew = true
					anyNewMu.Unlock()
				}

				return nil
			})
		}

		if err := grp.Err(); err != nil {
			return err
		}

		if !anyNew {
			logf("Reached the end of incremental import for @%s", r.username)
			return nil
		}

		if err := feed.Error(); err != nil && err != goinsta.ErrNoMore {
			return err
		}
	}

	logf("Reached the end of posts for @%s", r.username)
	return nil
}

// importPost adds new posts to the listNode.
// If the post was previously imported it doesn't do anything and just returns the existing ref.
func (r *run) importPost(listNode *importer.Object, post goinsta.Item) (fileRefStr string, alreadyHad bool, err error) {
	select {
	case <-r.Context().Done():
		return "", false, r.Context().Err()
	default:
	}

	newNode, err := listNode.ChildPathObject(post.ID)
	if err != nil {
		return "", false, err
	}

	if r.incremental && newNode.Attr(attrInstagramID) == post.ID {
		logf("Found post %s, skipping...", post.ID)
		return newNode.Attr(nodeattr.CamliContent), true, nil
	}

	ts := time.Unix(int64(post.TakenAt), 0)
	postTitle := fmt.Sprintf("Post on %s", ts.Format("02 Jan 2006, 15:04:05 MST"))
	if post.Caption.Text != "" {
		postTitle = post.Caption.Text
	}

	attrs := []string{
		importer.AcctAttrUserName, post.User.Username,
		nodeattr.Version, "1",
		nodeattr.Title, postTitle,
		nodeattr.Description, post.MediaToString(),
		nodeattr.DateModified, schema.RFC3339FromTime(time.Now()),
		nodeattr.URL, getInstagramURL(post),
		nodeattr.DateCreated, schema.RFC3339FromTime(ts),
	}

	if post.Location.Lat != 0 || post.Location.Lng != 0 {
		attrs = append(attrs,
			nodeattr.Latitude, strconv.FormatFloat(post.Location.Lat, 'f', -1, 64),
			nodeattr.Longitude, strconv.FormatFloat(post.Location.Lng, 'f', -1, 64),
		)
	}

	switch post.MediaType {
	case 1: // photo
		err = r.downloadPostContent(newNode, goinsta.GetBest(post.Images.Versions))

	case 2: // video
		err = r.downloadPostContent(newNode, goinsta.GetBest(post.Videos))

	case 8: // carousel
		attrs = append(attrs,
			nodeattr.Type, nodeTypeCarousel,
			attrCarouselItemCount, strconv.Itoa(len(post.CarouselMedia)),
		)
		var try = 0
		for i, item := range post.CarouselMedia {

			fileRefStr, alreadyHad, err := r.importPost(newNode, item)
			if err != nil {
				return "", alreadyHad, err
			}

			attrs = append(attrs, nodeattr.CamliPathOrderColon+strconv.Itoa(i+1), fileRefStr)

			// make the first carousel image the preview image
			if i == try && item.MediaType == 1 {
				attrs = append(attrs, nodeattr.CamliContentImage, fileRefStr)
			} else {
				try++
			}
		}

	default:
		err = fmt.Errorf("cannot handle post type %d", post.MediaType)
	}
	if err != nil {
		return "", false, err
	}

	changed, err := newNode.SetAttrs2(attrs...)
	if err == nil && changed {
		logf("Imported post %s to %s", post.ID, newNode.PermanodeRef())
	}

	return newNode.Attr(nodeattr.CamliContent), !changed, nil
}

func (r *run) downloadPostContent(node *importer.Object, urlstr string) error {
	if urlstr == "" {
		return errors.New("empty URL")
	}

	resp, err := ctxutil.Client(r.Context()).Get(urlstr)
	if err != nil {
		return err
	}

	if resp != nil && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed fetching post content %s with HTTP status %s", urlstr, resp.Status)
	}
	defer resp.Body.Close()

	u, err := url.Parse(urlstr)
	if err != nil {
		return err
	}
	h := blob.NewHash()
	fileRef, err := schema.WriteFileFromReader(r.Context(), r.Host.Target(), path.Base(u.Path), io.TeeReader(resp.Body, h))
	if err != nil {
		return err
	}
	fileRefStr := fileRef.String()
	wholeRef := blob.RefFromHash(h)

	// look for existing permanode and reuse if possible
	pn, attrs, err := findExistingPermanode(r.Context(), r.Host.Searcher(), wholeRef)
	if err != nil {
		if err != os.ErrNotExist {
			return fmt.Errorf("Error searching for permanode with %q as camliContent: %v", fileRefStr, err)
		}
	}

	// reuse existing content
	if err == nil && attrs != nil {
		if attrs.Get(nodeattr.CamliContent) != "" {
			fileRefStr = attrs.Get(nodeattr.CamliContent)
		}
		logf("Found permanode %q with identical camliContent, reusing that one...", pn)
	}

	// otherwise just use what we just downloaded
	if err := node.SetAttr(nodeattr.CamliContent, fileRefStr); err != nil {
		return err
	}
	return nil
}

func getInstagramURL(post goinsta.Item) string {
	return fmt.Sprintf("https://www.instagram.com/p/%s/", post.Code)
}

func findExistingPermanode(ctx context.Context, qs search.QueryDescriber, wholeRef blob.Ref) (pn blob.Ref, attrs url.Values, err error) {
	res, err := qs.Query(ctx, &search.SearchQuery{
		Constraint: &search.Constraint{
			Permanode: &search.PermanodeConstraint{
				Attr: nodeattr.CamliContent,
				ValueInSet: &search.Constraint{
					File: &search.FileConstraint{
						WholeRef: wholeRef,
					},
				},
			},
		},
		Describe: &search.DescribeRequest{
			Depth: 1,
		},
	})
	if err != nil {
		return pn, nil, err
	}
	if res.Describe == nil {
		return pn, nil, os.ErrNotExist
	}
	for _, resBlob := range res.Blobs {
		br := resBlob.Blob
		desBlob, ok := res.Describe.Meta[br.String()]
		if !ok || desBlob.Permanode == nil {
			continue
		}
		return br, desBlob.Permanode.Attr, nil
	}
	return pn, nil, os.ErrNotExist
}
