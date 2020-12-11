package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.springframework.util.StringUtils.hasText;

@Slf4j
public abstract class NetworkUtils {

    private static final String OS_NAME = System.getProperty("os.name").toLowerCase();

    public static void main(String... args) throws IOException {
        System.out.println(getTraceRouteTo(InetAddress.getByName("ya.ru")));
    }

    public static String getTraceRouteTo(InetAddress address) throws IOException {
        String fullTraceRouteCommand = getTraceRouteImpl() + " " + address.getHostAddress();

        Process traceRouteProcess = Runtime.getRuntime().exec(fullTraceRouteCommand);

        // read the output from the command
        String result = ioStreamToString(traceRouteProcess.getInputStream());

        // read any errors from the attempted command
        String errors = ioStreamToString(traceRouteProcess.getErrorStream());
        if (hasText(errors)) {
            log.error(errors);
        }

        return result;
    }

    private static String getTraceRouteImpl() {
        return isWindows() ? "tracert" : "traceroute";
    }

    private static String ioStreamToString(InputStream inputStream) throws IOException {
        Reader reader = new InputStreamReader(inputStream, selectCharset());
        Writer writer = new StringWriter();
        FileCopyUtils.copy(reader, writer);
        return writer.toString();
    }

    private static Charset selectCharset() {
        return isWindows() ? Charset.forName("CP866") : StandardCharsets.UTF_8;
    }

    private static boolean isWindows() {
        return OS_NAME.contains("win");
    }
}
