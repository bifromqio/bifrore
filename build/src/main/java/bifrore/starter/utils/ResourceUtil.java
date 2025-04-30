package bifrore.starter.utils;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ResourceUtil {

    public static File getFile(@NonNull String pathToResourceFile) {
        URL url = ResourceUtil.class.getClassLoader().getResource(pathToResourceFile);
        if (url != null) {
            log.trace("File found in resources: {}", pathToResourceFile);
            try {
                URI uri = url.toURI();
                return new File(uri);
            } catch (URISyntaxException e) {
                log.error("Failed to parse url:{}", url, e);
            }
        }
        return null;
    }

    public static File getFile(@NonNull String relativePathToFile, String sysPropOfDir) {
        if (sysPropOfDir != null && !sysPropOfDir.isEmpty()) {
            String dir = System.getProperty(sysPropOfDir);
            if (dir == null) {
                dir = System.getenv(sysPropOfDir);
            }
            if (dir != null) {
                File file = new File(dir, relativePathToFile);
                if (file.exists()) {
                    log.trace("File found in path: {}", file.getAbsolutePath());
                    return file;
                }
            }
        }
        log.warn("No dir or file found from sysPropOfDir:{}, fall back to classpath.", sysPropOfDir);
        return getFile(relativePathToFile);
    }

    public static ThreadFactory newThreadFactory(String name) {
        return new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                int s = seq.getAndIncrement();
                t.setName(s > 0 ? name + "-" + s : name);
                t.setDaemon(false);
                t.setPriority(Thread.NORM_PRIORITY);
                return t;
            }
        };
    }

    public static EventLoopGroup createEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(nThreads, threadFactory);
        }
        return new NioEventLoopGroup(nThreads, threadFactory);

    }
}
