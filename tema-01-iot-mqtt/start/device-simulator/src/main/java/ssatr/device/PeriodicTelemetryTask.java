package ssatr.device;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * TASK PERIODIC, cu perioada T = periodMs: fiecare job citește senzorul și
 * publică valoarea pe topic-ul de telemetrie.
 *
 * Dacă temperatura depășește pragul, taskul NU tratează el alarma, ci doar
 * semnalează un eveniment în coada alarmEvents. Alarma este tratată de
 * taskul sporadic.
 */
public class PeriodicTelemetryTask implements Runnable {

    private static final long NS_PER_MS = 1_000_000L;

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<Double> alarmEvents;

    // volatile: sunt citite aici, dar modificate de taskul aperiodic (alt thread)
    private volatile long periodMs = 2000;
    private volatile double threshold = 28.0;

    public PeriodicTelemetryTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                 BlockingQueue<Double> alarmEvents) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
    }

    @Override
    public void run() {
        // r_0: prima eliberare pe grila de timp
        long nextRelease = System.nanoTime();

        while (!Thread.currentThread().isInterrupted()) {
            if (!waitForRelease(nextRelease)) {
                return;                                  // thread oprit
            }
            runJob();
            // periodMs se citește la fiecare job (SET_INTERVAL îl poate schimba)
            nextRelease += periodMs * NS_PER_MS;
        }
    }

    /** Așteptare absolută: doarme doar cât a mai rămas până la eliberare. */
    private boolean waitForRelease(long releaseNs) {
        long remainingNs = releaseNs - System.nanoTime();
        if (remainingNs <= 0) {
            return true;                                 // suntem în urmă, pornim imediat
        }
        try {
            TimeUnit.NANOSECONDS.sleep(remainingNs);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Un job: citește senzorul, publică telemetria, semnalează depășirea pragului. */
    private void runJob() {
        double reading = sensor.read();
        mqtt.publish(Config.telemetryTopic(deviceId), buildPayload(reading));

        if (reading > threshold) {
            alarmEvents.offer(reading);                  // alarma o publică taskul sporadic
        }
    }

    /** Formatul cerut: deviceId;timestamp;temperatura (cu punct zecimal). */
    private String buildPayload(double value) {
        return String.format(Locale.US, "%s;%d;%.2f", deviceId, System.currentTimeMillis(), value);
    }

    public void setPeriodMs(long periodMs) {
        this.periodMs = periodMs;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getPeriodMs() {
        return periodMs;
    }

    public double getThreshold() {
        return threshold;
    }
}