package ssatr.service;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Ultima stare cunoscută a fiecărui device: ultima temperatură, momentul
 * ultimului mesaj, numărul de alarme etc.
 *
 * Este actualizată de thread-urile MessageProcessor și citită de CommandConsole,
 * deci accesul trebuie sincronizat.
 */
public class Dashboard {

    /** Starea unui singur device. Se modifică doar din metodele synchronized ale Dashboard-ului. */
    private static class DeviceState {
        double lastTemperature = Double.NaN;   // NaN = încă nu a sosit telemetrie
        long lastMessageAtMs;                  // momentul primirii ultimului mesaj, de orice tip
        int alarmCount;
    }

    // TreeMap: device-urile apar sortate după id în tabel
    private final Map<String, DeviceState> devices = new TreeMap<>();

    public synchronized void onTelemetry(String deviceId, double temperature) {
        DeviceState state = stateOf(deviceId);
        state.lastTemperature = temperature;
        state.lastMessageAtMs = System.currentTimeMillis();
    }

    public synchronized void onAlarm(String deviceId) {
        DeviceState state = stateOf(deviceId);
        state.alarmCount++;
        state.lastMessageAtMs = System.currentTimeMillis();
    }

    public synchronized void onStatus(String deviceId) {
        stateOf(deviceId).lastMessageAtMs = System.currentTimeMillis();
    }

    private DeviceState stateOf(String deviceId) {
        return devices.computeIfAbsent(deviceId, id -> new DeviceState());
    }

    /** Afișează un tabel cu starea tuturor device-urilor. */
    public void print() {
        System.out.print(render());       // tabelul se construiește sub lock, se afișează după
    }

    private synchronized String render() {
        if (devices.isEmpty()) {
            return "(niciun device nu a trimis mesaje)\n";
        }
        long now = System.currentTimeMillis();
        StringBuilder table = new StringBuilder();
        table.append(String.format("%-12s %-8s %-18s %s%n", "DEVICE", "TEMP", "ULTIMUL MESAJ", "ALARME"));
        for (Map.Entry<String, DeviceState> entry : devices.entrySet()) {
            DeviceState state = entry.getValue();
            String temperature = Double.isNaN(state.lastTemperature)
                    ? "-" : String.format(Locale.US, "%.2f", state.lastTemperature);
            table.append(String.format("%-12s %-8s %-18s %d%n",
                    entry.getKey(), temperature, formatAge(now - state.lastMessageAtMs), state.alarmCount));
        }
        return table.toString();
    }

    /** Vechimea unui mesaj, ca text: "acum 3 s" sau "acum 2 min 5 s". */
    private static String formatAge(long ageMs) {
        long seconds = Math.max(0, ageMs) / 1000;
        if (seconds < 60) {
            return "acum " + seconds + " s";
        }
        return "acum " + (seconds / 60) + " min " + (seconds % 60) + " s";
    }
}