import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.FlickrException;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.RequestContext;
import com.flickr4java.flickr.auth.Auth;
import com.flickr4java.flickr.auth.AuthInterface;
import com.flickr4java.flickr.auth.Permission;
import com.flickr4java.flickr.people.PeopleInterface;
import com.flickr4java.flickr.people.User;
import com.flickr4java.flickr.photos.Extras;
import com.flickr4java.flickr.photos.Photo;
import com.flickr4java.flickr.photos.PhotoList;
import com.flickr4java.flickr.photosets.Photoset;
import com.flickr4java.flickr.photosets.Photosets;
import com.flickr4java.flickr.photosets.PhotosetsInterface;
import com.flickr4java.flickr.uploader.UploadMetaData;
import com.flickr4java.flickr.util.AuthStore;
import com.flickr4java.flickr.util.FileAuthStore;
import org.apache.commons.imaging.formats.jpeg.exif.ExifRewriter;
import org.apache.commons.imaging.formats.tiff.constants.ExifTagConstants;
import org.apache.commons.imaging.formats.tiff.constants.TiffTagConstants;
import org.apache.commons.imaging.formats.tiff.write.TiffOutputDirectory;
import org.apache.commons.imaging.formats.tiff.write.TiffOutputSet;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.scribe.model.Token;
import org.scribe.model.Verifier;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    private ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2,
                Runtime.getRuntime().availableProcessors(),
                100,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue(Runtime.getRuntime().availableProcessors(), true),
                new ThreadPoolExecutor.CallerRunsPolicy());

    PeriodFormatter periodFormatter = new PeriodFormatterBuilder()
            .appendDays()
            .appendSuffix("d")
            .appendHours()
            .appendSuffix("h")
            .appendMinutes()
            .appendSuffix("m")
            .appendSeconds()
            .appendSuffix("s")
            .toFormatter();

    private Main() {
        System.out.println(String.format("Warming up with %d cores", Runtime.getRuntime().availableProcessors()));
        try {
            this.authStore = new FileAuthStore(new File(AUTH_DIR));
        } catch (FlickrException e) {
            throw new IllegalStateException("Couldn't initialize auth store", e);
        }
        flickr = new Flickr(API_KEY, SHARED_SECRET, new REST());
    }

    private void upload(Path source) throws IOException, FlickrException, SAXException, InterruptedException {
        System.out.println(String.format("Scanning albums at: %s", source));
        this.albums = findAlbumsToUpload(source);
        System.out.println(String.format("Found %d albums", albums.size()));

        if (albums.size() == 0) {
            System.out.println("No albums found to upload.");

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
                    System.out.print("Uploading " + e);
                    pathIdMap.put(e, uploadFile(e));
                    System.out.println(" done.");
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

    private Map<String, Photo> getPhotosForSet(String setId) throws FlickrException {
        Map<String, Photo> result = new HashMap<>();
        PhotosetsInterface pi = flickr.getPhotosetsInterface();
        int photosPage = 1;
        while (true) {
            PhotoList<Photo> photos = pi.getPhotos(setId, Extras.ALL_EXTRAS, 0, 500, photosPage);
            try {
                for (int i = 0; i < photos.size(); i++) {
                    Photo photo = photos.get(i);
                    result.put(photo.getId(), photo);
                }
            } catch (Exception e) {
                break;
            }
            if (photos.size() < 500) {
                break;
            }
            photosPage++;
        }
        return result;
    }

    private void download(Path target) throws FlickrException, IOException, SAXException, InterruptedException {
        System.out.println(String.format("Looking for NSID for user %s", USER_NAME));
        nsid = getNSID(USER_NAME);
        System.out.println(String.format("Found NSID %s", nsid));

        System.out.println(String.format("Authorizing requests for %s", nsid));
        authorizeRequests(nsid);
        System.out.println(String.format("Requests are now authorized"));

        System.out.println(String.format("Looking for albums"));
        Map<String, Photoset> photosets = getRemoteAlbums();
        System.out.println(String.format("Found %d albums", photosets.size()));

        long startedAt = System.currentTimeMillis();
        int currentAlbum = 1;
        AtomicInteger processedPhotos = new AtomicInteger(0);
        AtomicInteger downloadedPhotos = new AtomicInteger(0);
        for (Map.Entry<String, Photoset> entry : photosets.entrySet()) {
            String setName = entry.getKey();
            Photoset photoset = entry.getValue();
            Map<String, Photo> photosForSet = getPhotosForSet(photoset.getId());

            System.out.print(String.format("\n(%s) %d photos processed so far. Now processing album [%d of %d] %s with %d photos ",
                    periodFormatter.print(new Duration(System.currentTimeMillis() - startedAt).toPeriod()),
                    processedPhotos.get(),
                    currentAlbum++,
                    photosets.size(),
                    setName,
                    photosForSet.size()));

            Path setFolder = target.resolve(setName);
            setFolder.toFile().mkdirs();

            photosForSet.values().stream()
                    .forEach(e -> threadPoolExecutor.submit(() -> {
                        processedPhotos.incrementAndGet();
                        if (this.downloadPhoto(setFolder, e)) {
                            downloadedPhotos.getAndIncrement();
                            System.out.print("+");
                        } else {
                            System.out.print("-");
                        }
                    }));
        }
        threadPoolExecutor.shutdown();
        while (!threadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            System.out.println("Awaiting completion of threads.");
        }
        System.out.println(String.format("Downloaded a total of %d photos. %d processed. Duration: ",
                downloadedPhotos.get(),
                processedPhotos.get(),
                periodFormatter.print(new Duration(System.currentTimeMillis() - startedAt).toPeriod())));
    }

    private boolean downloadPhoto(Path setFolder, Photo photo) {
        File file = setFolder.resolve("_flicked_" + photo.getId() + ".jpg").toFile();
        if (file.exists()) {
            return false;
        }

        try {
            URL url = new URL(photo.getOriginalUrl());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.connect();
            try (InputStream is = new BufferedInputStream(conn.getInputStream());
                 OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
                String formattedDateTaken = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(photo.getDateTaken() == null ? new Date() : photo.getDateTaken());
                TiffOutputSet outputSet = new TiffOutputSet();
                TiffOutputDirectory rootDirectory = outputSet.getOrCreateRootDirectory();
                rootDirectory.add(TiffTagConstants.TIFF_TAG_DATE_TIME,
                        formattedDateTaken);
                TiffOutputDirectory tiffDirectory = outputSet.getOrCreateExifDirectory();
                tiffDirectory.add(ExifTagConstants.EXIF_TAG_DATE_TIME_ORIGINAL,
                        formattedDateTaken);
                tiffDirectory.add(ExifTagConstants.EXIF_TAG_SUB_SEC_TIME_ORIGINAL,
                        "00");
                tiffDirectory.add(ExifTagConstants.EXIF_TAG_DATE_TIME_DIGITIZED,
                        formattedDateTaken);
                tiffDirectory.add(ExifTagConstants.EXIF_TAG_SUB_SEC_TIME_DIGITIZED,
                        "00");
                new ExifRewriter().updateExifMetadataLossy(is, os, outputSet);
                return true;
            }
        } catch (Exception e) {
            System.out.println(String.format("\nUnable to download %s", photo.getId()));
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String... args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Inform option and path Eg: java Main --upload C:\\temp");
        }

        Main main = new Main();

        if ("--download".equals(args[0])) {
            main.download(Paths.get(args[1]));
        } else if ("--upload".equals(args[0])) {
            main.upload(Paths.get(args[1]));
        } else {
            throw new IllegalArgumentException("First argument must be either --download or --upload");
        }
    }

    private boolean isOnFlickr(Path path) {
        if (path.getFileName().toString().startsWith("_flicked_")) {
            return true;
        }
        if (path.getFileName().toString().matches("\\d{8}_\\d{1,9}\\.jpg")) {
            return true;
        }
        return false;
    }

    private Map<String, List<Path>> findAlbumsToUpload(Path path) throws IOException {
        return Files.find(path, 5, (p, a) -> p.toString().endsWith("jpg") && !isOnFlickr(p))
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
        file.renameTo(path.getParent().resolve("_flicked_" + System.currentTimeMillis() + path.getFileName()).toFile());
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
