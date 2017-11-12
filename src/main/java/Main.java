import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.FlickrException;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.RequestContext;
import com.flickr4java.flickr.auth.Auth;
import com.flickr4java.flickr.auth.AuthInterface;
import com.flickr4java.flickr.auth.Permission;
import com.flickr4java.flickr.people.PeopleInterface;
import com.flickr4java.flickr.people.User;
import com.flickr4java.flickr.photosets.Photoset;
import com.flickr4java.flickr.photosets.Photosets;
import com.flickr4java.flickr.photosets.PhotosetsInterface;
import com.flickr4java.flickr.uploader.UploadMetaData;
import com.flickr4java.flickr.util.AuthStore;
import com.flickr4java.flickr.util.FileAuthStore;
import jdk.internal.org.xml.sax.SAXException;
import org.scribe.model.Token;
import org.scribe.model.Verifier;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    private static final String API_KEY = "33883515a37071d1a689debb0fc2f697";
    private static final String SHARED_SECRET = "17910a318c40863f";
    private static final String USER_NAME = System.getProperty("user.override", "itamarjuniorvieira");
    private static final String AUTH_DIR = System.getProperty("user.home") + File.separatorChar + ".flickrAuth";

    private AuthStore authStore;
    private String nsid;
    private Flickr flickr;
    private Map<String, List<Path>> albums;

    private Main() {
        try {
            this.authStore = new FileAuthStore(new File(AUTH_DIR));
        } catch (FlickrException e) {
            throw new IllegalStateException("Couldn't initialize auth store", e);
        }
        flickr = new Flickr(API_KEY, SHARED_SECRET, new REST());
    }

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Inform source path. Eg: java Main C:/temp");
        }

        Main main = new Main();
        main.doIt(Paths.get(args[0]));
    }

    private void doIt(Path source) throws IOException, FlickrException, SAXException {
        System.out.println(String.format("Scanning albums at: %s", source));
        this.albums = findAlbumsToUpload(source);
        System.out.println(String.format("Found %d albums", albums.size()));

        if (albums.size() == 0) {
            System.out.println("We're done here");
            return;
        }

        System.out.println(String.format("Looking for NSID for user %s", USER_NAME));
        nsid = getNSID(USER_NAME);
        System.out.println(String.format("Found NSID %s", nsid));

        System.out.println(String.format("Authorizing requests for %s", nsid));
        authorizeRequests(nsid);
        System.out.println(String.format("Requests are now authorized"));

        System.out.println(String.format("Uploading pictures..."));
        Map<Path, String> pathIdMap = new HashMap<>();
        albums.values().stream()
                .flatMap(e -> e.stream())
                .forEach(e -> {
                    System.out.print(String.format("  Uploading picture %s", e));
                    pathIdMap.put(e, uploadFile(e));
                    System.out.println(String.format(" done!"));
                });
        System.out.println(String.format("Uploaded %d pictures", pathIdMap.size()));

        System.out.println(String.format("Looking for existing albums"));
        Map<String, Photoset> photosets = getRemoteAlbums();
        System.out.println(String.format("Found %d albums", photosets.size()));

        albums.keySet().stream()
                .filter(e -> !photosets.keySet().contains(e))
                .forEach(e -> {
                    System.out.print(String.format("Creating album %s", e));
                    Photoset photoSet = createRemoteAlbum(e, pathIdMap.get(albums.get(e).get(0)));
                    photosets.put(e, photoSet);
                    System.out.println(String.format(" done!"));
                });

        System.out.println(String.format("Adding photos to albums"));
        for (Map.Entry<String, List<Path>> entry : albums.entrySet()) {
            Photoset photoset = photosets.get(entry.getKey());
            System.out.println(String.format("Adding photos to album %s", entry.getKey()));
            for (Path path : entry.getValue()) {
                System.out.print(String.format("  Adding photo %s", path.getFileName()));
                addPhotoToRemoteAlbum(photoset.getId(), pathIdMap.get(path));
                System.out.println(String.format(" done!"));
            }
        }
    }

    private Map<String, List<Path>> findAlbumsToUpload(Path path) throws IOException {
        return Files.find(path, 2, (p, a) -> p.toString().endsWith("jpg") && !p.getFileName().toString().startsWith("_flicked_"))
                .collect(Collectors.groupingBy(p -> p.getParent().getFileName().toString()));
    }

    private String getNSID(String userName) throws FlickrException {
        Auth auth = null;
        if (authStore != null) {
            auth = authStore.retrieve(userName);

            if (auth != null) {
                return auth.getUser().getId();
            }
        }
        if (auth != null)
            return auth.getUser().getId();

        Auth[] allAuths = authStore.retrieveAll();
        for (int i = 0; i < allAuths.length; i++) {
            if (userName.equals(allAuths[i].getUser().getUsername())) {
                return allAuths[i].getUser().getId();
            }
        }

        PeopleInterface peopleInterf = flickr.getPeopleInterface();
        User u = peopleInterf.findByUsername(userName);
        if (u != null) {
            return u.getId();
        }

        throw new IllegalArgumentException(String.format("Couldn't find user %s", userName));
    }

    private void requestAuthorization() throws IOException, SAXException, FlickrException {
        AuthInterface authInterface = flickr.getAuthInterface();
        Token accessToken = authInterface.getRequestToken();

        // Try with DELETE permission. At least need write permission for upload and add-to-set.
        String url = authInterface.getAuthorizationUrl(accessToken, Permission.DELETE);
        System.out.println("Follow this URL to authorise yourself on Flickr");
        System.out.println(url);
        System.out.println("Paste in the token it gives you:");
        System.out.print(">>");

        Scanner scanner = new Scanner(System.in);
        String tokenKey = scanner.nextLine();

        Token requestToken = authInterface.getAccessToken(accessToken, new Verifier(tokenKey));

        Auth auth = authInterface.checkToken(requestToken);
        RequestContext.getRequestContext().setAuth(auth);
        this.authStore.store(auth);
        scanner.close();
        System.out.println("Thanks.  You probably will not have to do this every time. Auth saved for user: " + auth.getUser().getUsername() + " nsid is: "
                + auth.getUser().getId());
        System.out.println(" AuthToken: " + auth.getToken() + " tokenSecret: " + auth.getTokenSecret());
    }

    public void authorizeRequests(String nsid) throws IOException, SAXException, FlickrException {
        RequestContext rc = RequestContext.getRequestContext();

        if (this.authStore != null) {
            Auth auth = this.authStore.retrieve(nsid);
            if (auth == null) {
                this.requestAuthorization();
            } else {
                rc.setAuth(auth);
            }
        }
    }

    public Map<String, Photoset> getRemoteAlbums() throws FlickrException {
        Map<String, Photoset> result = new HashMap<>();
        PhotosetsInterface pi = flickr.getPhotosetsInterface();
        int setsPage = 1;
        while (true) {
            Photosets photosets = pi.getList(nsid, 500, setsPage, null);
            Collection<Photoset> setsColl = photosets.getPhotosets();
            Iterator<Photoset> setsIter = setsColl.iterator();
            while (setsIter.hasNext()) {
                Photoset set = setsIter.next();
                result.put(set.getTitle(), set);
            }

            if (setsColl.size() < 500) {
                break;
            }
            setsPage++;
        }
        return result;
    }


    private Photoset createRemoteAlbum(String name, String primaryPhotoId) {
        PhotosetsInterface pi = flickr.getPhotosetsInterface();
        try {
            return pi.create(name, String.format("Album created by %s", USER_NAME), primaryPhotoId);
        } catch (FlickrException e) {
            throw new IllegalStateException(String.format("Unable to create album %s", name));
        }
    }

    public String uploadFile(Path path) {
        UploadMetaData metaData = new UploadMetaData();

        metaData.setPublicFlag(false);
        metaData.setFriendFlag(false);
        metaData.setFamilyFlag(true);
        metaData.setTitle(path.getFileName().toString());
        metaData.setFilename(path.getFileName().toString());

        File file = path.toFile();
        String photoId;
        try {
            photoId = flickr.getUploader().upload(file, metaData);
        } catch (FlickrException e) {
            throw new IllegalStateException(String.format("Unable to upload picture %s", path), e);
        }
        file.renameTo(path.getParent().resolve("_flicked_" + path.getFileName()).toFile());
        return photoId;
    }

    private void addPhotoToRemoteAlbum(String photoSetId, String photoId) {
        PhotosetsInterface pi = flickr.getPhotosetsInterface();
        try {
            pi.addPhoto(photoSetId, photoId);
        } catch (FlickrException e) {
            if ("3".equals(e.getErrorCode())) {
                // We can just ignore, photo is already in set
            } else {
                throw new IllegalStateException(String.format("Unable to add photo %s to album %s", photoId, photoSetId), e);
            }
        }
    }

}
