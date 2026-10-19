package ssatr.service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * Preia mesajele de un anumit tip ("telemetry", "alarm" sau "status") din coada
 * inbox, le salvează și actualizează dashboard-ul.
 *
 * Formatul mesajelor (câmpuri separate prin ";"):
 *   telemetry:  deviceId;timestamp;temperatura
 *   alarm:      deviceId;timestamp;temperatura;prag
 *   status:     deviceId;timestamp;text
 */
public class MessageProcessor implements Runnable {

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private final String kind;
    private final BlockingQueue<String> inbox;
    private final CsvStorage storage;
    private final Dashboard dashboard;

    public MessageProcessor(String kind, BlockingQueue<String> inbox, CsvStorage storage, Dashboard dashboard) {
        this.kind = kind;
        this.inbox = inbox;
        this.storage = storage;
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            String message;
            try {
                message = inbox.take();                    // așteaptă următorul mesaj
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            try {
                handle(message);
            } catch (IllegalArgumentException e) {
                // mesaj cu format invalid: îl ignorăm, thread-ul continuă
                System.out.println("[" + kind + "] mesaj ignorat (" + e.getMessage() + "): " + message);
            } catch (RuntimeException e) {
                System.out.println("[" + kind + "] eroare neasteptata la procesare: " + e);
            }
        }
    }

    private void handle(String message) {
        switch (kind) {
            case "telemetry" -> handleTelemetry(message);
            case "alarm" -> handleAlarm(message);
            case "status" -> handleStatus(message);
            default -> throw new IllegalArgumentException("tip necunoscut: " + kind);
        }
    }

    private void handleTelemetry(String message) {
        String[] f = splitFields(message, 3);
        parseTimestamp(f[1]);
        double temperature = parseNumber(f[2], "temperatura");

        storage.append(kind, message);                     // salvat exact cum a sosit, dar doar dacă e valid
        dashboard.onTelemetry(f[0], temperature);
    }

    private void handleAlarm(String message) {
        String[] f = splitFields(message, 4);
        long timestamp = parseTimestamp(f[1]);
        double temperature = parseNumber(f[2], "temperatura");
        double threshold = parseNumber(f[3], "pragul");

        storage.append(kind, message);
        dashboard.onAlarm(f[0]);
        System.out.printf(Locale.US, "[ALARMA] %s la %s: temperatura %.2f depaseste pragul %.2f%n",
                f[0], TIME.format(Instant.ofEpochMilli(timestamp)), temperature, threshold);
    }

    private void handleStatus(String message) {
        String[] f = splitFields(message, 3);              // textul e ultimul câmp
        long timestamp = parseTimestamp(f[1]);

        storage.append(kind, message);
        dashboard.onStatus(f[0]);
        System.out.printf("[STATUS] %s la %s: %s%n", f[0], TIME.format(Instant.ofEpochMilli(timestamp)), f[2]);
    }

    /** Desparte mesajul în exact `expected` câmpuri; ultimul câmp primește tot restul. */
    private static String[] splitFields(String message, int expected) {
        String[] fields = message.split(";", expected);
        if (fields.length != expected) {
            throw new IllegalArgumentException("se asteptau " + expected + " campuri, au venit " + fields.length);
        }
        fields[0] = fields[0].trim();
        if (fields[0].isEmpty()) {
            throw new IllegalArgumentException("deviceId lipseste");
        }
        return fields;
    }

    private static long parseTimestamp(String text) {
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("timestamp invalid: " + text);
        }
    }

    private static double parseNumber(String text, String what) {
        try {
            double value = Double.parseDouble(text.trim());
            if (!Double.isFinite(value)) {
                throw new NumberFormatException();
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(what + " invalida: " + text);
        }
    }
}