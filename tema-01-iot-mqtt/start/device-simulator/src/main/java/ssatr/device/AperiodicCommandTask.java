package ssatr.device;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * TASK APERIODIC: execută comenzile primite de la business service.
 *
 * Comenzile sosesc oricând, pe MQTT, fără un interval minim între ele, și sunt
 * puse în coada commands. Fiecare comandă eliberează un job al taskului.
 * Comenzi care trebuie suportate:
 *
 *   SET_INTERVAL <ms>       schimbă perioada T a taskului periodic
 *   SET_THRESHOLD <valoare> schimbă pragul de alarmă
 *   RESET                   readuce senzorul la valoarea inițială
 *   STATUS                  raportează perioada și pragul curente
 *
 * După executarea fiecărei comenzi, device-ul publică pe Config.statusTopic(deviceId)
 * o confirmare: "deviceId;timestamp;text", de exemplu
 * "device-1;1727600000000;OK SET_INTERVAL 1000".
 */
public class AperiodicCommandTask implements Runnable {

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<String> commands;
    private final PeriodicTelemetryTask periodic;

    public AperiodicCommandTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                BlockingQueue<String> commands, PeriodicTelemetryTask periodic) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.commands = commands;
        this.periodic = periodic;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            String command;
            try {
                command = commands.take();                 // așteaptă o comandă, în ordinea sosirii
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            String reply;
            try {
                reply = execute(command);
            } catch (IllegalArgumentException e) {
                reply = "ERROR " + e.getMessage();         // comandă greșită: raportăm, nu oprim taskul
            } catch (RuntimeException e) {
                reply = "ERROR eroare interna " + e.getClass().getSimpleName();
            }
            publishStatus(reply);
        }
    }

    /** Execută o comandă și întoarce textul confirmării. Aruncă IllegalArgumentException dacă e greșită. */
    private String execute(String command) {
        String[] parts = command.trim().split("\\s+");
        String name = parts[0].toUpperCase(Locale.ROOT);

        if (name.isEmpty()) {
            throw new IllegalArgumentException("comanda goala");
        }
        return switch (name) {
            case "SET_INTERVAL" -> setInterval(parts);
            case "SET_THRESHOLD" -> setThreshold(parts);
            case "RESET" -> reset(parts);
            case "STATUS" -> status(parts);
            default -> throw new IllegalArgumentException("comanda necunoscuta " + name);
        };
    }

    private String setInterval(String[] parts) {
        String argument = exactlyOneArgument(parts);
        long ms;
        try {
            ms = Long.parseLong(argument);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("valoare invalida pentru SET_INTERVAL: " + argument);
        }
        if (ms <= 0) {
            throw new IllegalArgumentException("intervalul trebuie sa fie pozitiv: " + ms);
        }
        periodic.setPeriodMs(ms);
        return "OK SET_INTERVAL " + ms;
    }

    private String setThreshold(String[] parts) {
        String argument = exactlyOneArgument(parts);
        double value;
        try {
            value = Double.parseDouble(argument);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("valoare invalida pentru SET_THRESHOLD: " + argument);
        }
        if (!Double.isFinite(value)) {                      // respinge NaN si Infinity
            throw new IllegalArgumentException("valoare invalida pentru SET_THRESHOLD: " + argument);
        }
        periodic.setThreshold(value);
        return String.format(Locale.US, "OK SET_THRESHOLD %.2f", value);
    }

    private String reset(String[] parts) {
        noArguments(parts);
        sensor.reset();
        return "OK RESET";
    }

    private String status(String[] parts) {
        noArguments(parts);
        return String.format(Locale.US, "OK STATUS interval=%d threshold=%.2f",
                periodic.getPeriodMs(), periodic.getThreshold());
    }

    private String exactlyOneArgument(String[] parts) {
        String name = parts[0].toUpperCase(Locale.ROOT);
        if (parts.length < 2) {
            throw new IllegalArgumentException("lipseste parametrul pentru " + name);
        }
        if (parts.length > 2) {
            throw new IllegalArgumentException("prea multi parametri pentru " + name);
        }
        return parts[1];
    }

    private void noArguments(String[] parts) {
        if (parts.length > 1) {
            throw new IllegalArgumentException("comanda " + parts[0].toUpperCase(Locale.ROOT) + " nu are parametri");
        }
    }

    /** Publică confirmarea: deviceId;timestamp;text. Textul nu are voie să conțină ';' (separator). */
    private void publishStatus(String reply) {
        String safeReply = reply.replace(';', ',');
        String message = deviceId + ";" + System.currentTimeMillis() + ";" + safeReply;
        mqtt.publish(Config.statusTopic(deviceId), message);
        System.out.println("[aperiodic] " + message);
    }
}