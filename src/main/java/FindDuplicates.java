import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import org.apache.commons.imaging.ImageReadException;
import org.apache.commons.imaging.Imaging;
import org.apache.commons.imaging.formats.jpeg.JpegImageMetadata;
import org.apache.commons.imaging.formats.tiff.TiffField;
import org.apache.commons.imaging.formats.tiff.constants.TiffTagConstants;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class FindDuplicates {
    private final Path target;
    private List<Path> folders;
    private Multimap<Float, ImageInfo> groupedByMagic;
    private Map<Integer, Collection<ImageInfo>> groupedByGroupId;
    private AtomicInteger groupIdGenerator = new AtomicInteger();
    private Map<Path, String> lookLater = new HashMap<>();

    private ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2,
            Runtime.getRuntime().availableProcessors(),
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue(100000, true),
            new ThreadPoolExecutor.CallerRunsPolicy());
    private Set<ImageInfo> uniqueSet;

    protected FindDuplicates(Path target, Path... folders) {
        this.target = target;
        this.folders = Arrays.asList(folders);
    }

    private void groupSimilar() {
        this.groupedByGroupId = new ConcurrentHashMap<>();
        for (float magic : groupedByMagic.keySet()) {
            Collection<ImageInfo> allSameMagic = groupedByMagic.get(magic);
            for (ImageInfo imageInfoOuter : allSameMagic) {
                if (imageInfoOuter.getGroupId() > 0) {
                    continue;
                }
                Set<ImageInfo> duplicates = allSameMagic.stream()
                        .filter(e -> e.getGroupId() < 0)
                        .filter(e -> e.possibleDuplicate(imageInfoOuter))
                        .collect(Collectors.toSet());

                if (!duplicates.isEmpty()) {
                    int groupId = groupIdGenerator.getAndIncrement();
                    duplicates = Streams.concat(duplicates.stream(), Stream.of(imageInfoOuter))
                            .map(e -> {
                                e.setGroupId(groupId);
                                return e;
                            })
                            .collect(Collectors.toSet());
                    groupedByGroupId.put(groupId, duplicates);
                }
            }
        }
        this.uniqueSet = groupedByMagic.values().stream()
                .filter(e -> e.getGroupId() < 0)
                .collect(Collectors.toSet());
    }

    private void loadFiles() throws IOException {
        AtomicInteger discoveredFiles = new AtomicInteger(0);
        AtomicInteger processedFiles = new AtomicInteger(0);
        this.groupedByMagic = ArrayListMultimap.create();
        for (Path path : folders) {
            Files.walk(path, 10)
                    .filter(e -> e.toString().toLowerCase().endsWith(".jpg"))
                    .peek(e -> {
                        if (discoveredFiles.get() % 1000 == 0) {
                            System.out.println(String.format("Files discovered: %d / Files processed: %d", discoveredFiles.get(), processedFiles.get()));
                        }
                    })
                    .forEach(e -> {
                        discoveredFiles.getAndIncrement();
                        threadPoolExecutor.submit(() -> {
                            try {
                                ImageInfo imageInfo = new ImageInfo(e.toFile());
                                groupedByMagic.put(imageInfo.getAverage(), imageInfo);
                            } catch (Exception x) {
                                lookLater.put(e, x.getMessage());
                                throw new IllegalStateException(x);
                            } finally {
                                processedFiles.getAndIncrement();
                            }
                        });
                    });
        }
        while (discoveredFiles.get() != processedFiles.get()) {
            try {
                Thread.sleep(5000);
                System.out.println(String.format("Files discovered: %d / Files processed: %d", discoveredFiles.get(), processedFiles.get()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void doIt() throws IOException {
        System.out.println("Loading files");
        loadFiles();
        System.out.println("Grouping similar");
        groupSimilar();
//        System.out.println(groupedByMagic);
//        System.out.println(groupedByGroupId);
        System.out.println(String.format("Total number of unique: %d", groupedByGroupId.size()));
        System.out.println(String.format("Total number of duplicate groups: %d", uniqueSet.size()));
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("Storing files");
        storeFiles();
        System.out.println("----------------------------TO CHECK----------------");
        System.out.println(lookLater);
    }

    private void storeFile(ImageInfo imageInfo, AtomicInteger filesToStore, AtomicInteger filesStored) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMdd");
        Path sourcePath = imageInfo.getFile().toPath();
        Path albumName = sourcePath.getParent().getFileName();
        String targetFileName = String.format("%s_%d.jpg", df.format(imageInfo.getDateTaken()), filesToStore.getAndIncrement());
        Path targetFolder = target.resolve(albumName);
        targetFolder.toFile().mkdirs();
        threadPoolExecutor.submit(() -> {
            try {
                Files.copy(imageInfo.file.toPath(), targetFolder.resolve(targetFileName));
            } catch (Exception e) {
                lookLater.put(imageInfo.file.toPath(), e.getMessage());
                throw new IllegalStateException(e);
            } finally {
                filesStored.getAndIncrement();
            }
        });
    }

    private void storeFiles() throws IOException {
        AtomicInteger filesToStore = new AtomicInteger(0);
        AtomicInteger filesStored = new AtomicInteger(0);
        uniqueSet.forEach(e -> storeFile(e, filesToStore, filesStored));

        for (Collection<ImageInfo> duplicates : groupedByGroupId.values()) {
            duplicates.stream()
                    .sorted((e1, e2) -> {
                        try {
                            BasicFileAttributes attr1 = Files.readAttributes(e1.getFile().toPath(), BasicFileAttributes.class);
                            BasicFileAttributes attr2 = Files.readAttributes(e2.getFile().toPath(), BasicFileAttributes.class);
                            return attr1.creationTime().compareTo(attr2.creationTime());
                        } catch (IOException e) {
                            lookLater.put(e1.file.toPath(), e.getMessage());
                            lookLater.put(e2.file.toPath(), e.getMessage());
                            throw new IllegalStateException(e);
                        }
                    })
                    .findFirst()
                    .ifPresent(e -> storeFile(e, filesToStore, filesStored));
        }

        while (filesToStore.get() != filesStored.get()) {
            try {
                Thread.sleep(5000);
                System.out.println(String.format("Stored %d of %d files", filesStored.get(), filesToStore.get()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String... args) throws Exception {
        new FindDuplicates(Paths.get("C:\\target"), Paths.get("C:\\Temp"), Paths.get("D:\\Itamar\\Pictures")).doIt();
    }

    class ImageInfo {
        private int groupId = -1;
        private float average;
        private byte[] histogram = new byte[256];
        private LocalDate dateTaken;
        private File file;
        private List<DateTimeFormatter> knownFormats = ImmutableList
                .of(DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                        DateTimeFormatter.ofPattern("yyyy:MM:dd"));

        public ImageInfo(File file) {
            this.file = file;
            loadInfo();
        }

        private void loadInfo() {
            int[] pixelCount = new int[256];
            int numberOfPixels;
            try {
                BufferedImage bufferedImage = ImageIO.read(new BufferedInputStream(new FileInputStream(file)));
                for (int x = 0; x < bufferedImage.getWidth(); x++) {
                    for (int y = 0; y < bufferedImage.getHeight(); y++) {
                        int p = bufferedImage.getRGB(x, y);
                        int r = (p >> 16) & 0xff;
                        int g = (p >> 8) & 0xff;
                        int b = p & 0xff;
                        pixelCount[(r + g + b) / 3]++;
                    }
                }
                numberOfPixels = bufferedImage.getHeight() * bufferedImage.getWidth();

                final JpegImageMetadata jpegMetadata = (JpegImageMetadata) Imaging.getMetadata(file);
                if (jpegMetadata != null) {
                    final TiffField field = jpegMetadata.findEXIFValueWithExactMatch(TiffTagConstants.TIFF_TAG_DATE_TIME);
                    if (field != null) {
                        for (DateTimeFormatter format : knownFormats) {
                            try {
                                dateTaken = LocalDate.parse(field.getStringValue().substring(0, 10), format);
                                break;
                            } catch (DateTimeParseException e) {
                                // Ignore
                            }
                        }
                    }
                }
                if (dateTaken == null) {
                    dateTaken = LocalDate.now();
                }

            } catch (IOException | ImageReadException e) {
                lookLater.put(file.toPath(), e.getMessage());
                throw new IllegalStateException("Error loading file", e);
            }
            float sum = 0;
            for (int i = 0; i < pixelCount.length - 1; i++) {
                histogram[i] = (byte) (pixelCount[i] * 255 / numberOfPixels);
                sum += histogram[i];
            }
            average = sum / numberOfPixels;

//            System.out.println(Arrays.toString(pixelCount));
//            System.out.println(Arrays.toString(histogram));
//            System.out.println(average);
//            System.out.println(dateTaken);
        }

        public float getAverage() {
            return average;
        }

        public LocalDate getDateTaken() {
            return dateTaken;
        }

        public int getGroupId() {
            return groupId;
        }

        public void setGroupId(int groupId) {
            this.groupId = groupId;
        }

        public File getFile() {
            return file;
        }

        public boolean possibleDuplicate(ImageInfo other) {
            if (other == this) {
                return false;
            }

            if (average != other.average) {
//                System.out.println("Not same magic");
                return false;
            }
            if (!dateTaken.equals(other.dateTaken)) {
//                System.out.println("Not same date");
                return false;
            }
            if (!Arrays.equals(histogram, other.histogram)) {
//                System.out.println("Different histogram");
                return false;
            }

            return true;
        }

        public String toString() {
            return String.format("{%d,%f,%tD,%s", groupId, average, dateTaken, file);
        }
    }
}
