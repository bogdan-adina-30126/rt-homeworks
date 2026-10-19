package ssatr.device;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * TASK SPORADIC: tratează alarmele de temperatură.
 *
 * Evenimentele sosesc în coada alarmEvents (puse de taskul periodic).
 * Intervalul minim între sosiri este MIN_INTERARRIVAL_MS: între două eliberări
 * ale taskului (două alarme publicate) trebuie să treacă cel puțin atât.
 * Intervalul este impus de cod: evenimentele care sosesc mai des sunt
 * respinse, ca să nu fie inundat brokerul.
 */
public class SporadicAlarmTask implements Runnable {

    static final long MIN_INTERARRIVAL_MS = 5000;
    private static final long NS_PER_MS = 1_000_000L;

    private final String deviceId;
    private final MqttConnection mqtt;
    private final BlockingQueue<Double> alarmEvents;
    // pragul îl citim din taskul periodic: el este singurul loc unde se schimbă (SET_THRESHOLD)
    private final PeriodicTelemetryTask periodic;

    private boolean hasReleased = false;   // a existat deja o alarmă publicată?
    private long lastReleaseNs;            // momentul ultimei eliberări (timp monoton)

    public SporadicAlarmTask(String deviceId, MqttConnection mqtt, BlockingQueue<Double> alarmEvents,
                             PeriodicTelemetryTask periodic) {
        this.deviceId = deviceId;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
        this.periodic = periodic;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            double temperature;
            try {
                temperature = alarmEvents.take();          // blocat până sosește un eveniment
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            long arrivalNs = System.nanoTime();            // momentul sosirii evenimentului

            if (violatesMinInterarrival(arrivalNs)) {
                long sinceLastMs = (arrivalNs - lastReleaseNs) / NS_PER_MS;
                System.out.printf("[sporadic] alarmă RESPINSĂ: au trecut doar %d ms (interval minim %d ms)%n",
                        sinceLastMs, MIN_INTERARRIVAL_MS);
                continue;
            }

            lastReleaseNs = arrivalNs;                     // eliberare nouă: jobul se execută
            hasReleased = true;
            publishAlarm(temperature);
        }
    }

    /** Adevărat dacă evenimentul vine prea repede după ultima alarmă publicată. */
    private boolean violatesMinInterarrival(long arrivalNs) {
        return hasReleased && (arrivalNs - lastReleaseNs) < MIN_INTERARRIVAL_MS * NS_PER_MS;
    }

    /** Formatul cerut: deviceId;timestamp;temperatura;prag */
    private void publishAlarm(double temperature) {
        String message = String.format(Locale.US, "%s;%d;%.2f;%.2f",
                deviceId, System.currentTimeMillis(), temperature, periodic.getThreshold());
        mqtt.publish(Config.alarmTopic(deviceId), message);
        System.out.println("[sporadic] alarmă publicată: " + message);
    }
}