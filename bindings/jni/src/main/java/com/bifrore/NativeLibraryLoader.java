package com.bifrore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

final class NativeLibraryLoader {
    private NativeLibraryLoader() {}

    static void load() {
        String os = detectOs();
        String arch = detectArch();
        String ext = libraryExtension(os);
        String base = "/natives/" + os + "-" + arch + "/";

        Path tempDir;
        try {
            tempDir = Files.createTempDirectory("bifrore-natives-");
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("failed to create temp dir for native libraries: " + e);
        }
        tempDir.toFile().deleteOnExit();

        Path rustLib = extract(base + "libbifrore_embed." + ext, tempDir);
        Path jniLib = extract(base + "libbifrore_jni." + ext, tempDir);

        System.load(rustLib.toAbsolutePath().toString());
        System.load(jniLib.toAbsolutePath().toString());
    }

    private static Path extract(String resourcePath, Path tempDir) {
        String fileName = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        Path target = tempDir.resolve(fileName);
        try (InputStream input = NativeLibraryLoader.class.getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new UnsatisfiedLinkError("missing native resource: " + resourcePath);
            }
            Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
            target.toFile().deleteOnExit();
            return target;
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("failed to extract native resource " + resourcePath + ": " + e);
        }
    }

    private static String detectOs() {
        String name = System.getProperty("os.name", "").toLowerCase();
        if (name.contains("mac")) {
            return "darwin";
        }
        if (name.contains("linux")) {
            return "linux";
        }
        throw new UnsatisfiedLinkError("unsupported os: " + name);
    }

    private static String detectArch() {
        String arch = System.getProperty("os.arch", "").toLowerCase();
        if (arch.equals("aarch64") || arch.equals("arm64")) {
            return "aarch64";
        }
        if (arch.equals("x86_64") || arch.equals("amd64")) {
            return "x86_64";
        }
        throw new UnsatisfiedLinkError("unsupported arch: " + arch);
    }

    private static String libraryExtension(String os) {
        return switch (os) {
            case "darwin" -> "dylib";
            case "linux" -> "so";
            default -> throw new UnsatisfiedLinkError("unsupported os: " + os);
        };
    }
}
