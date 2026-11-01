// Device-ul IoT din tema 01, de data aceasta pe un ESP32-C3 Super Mini.
//
// Aceeași logică și același protocol ca device-simulator din tema 01, cu trei
// taskuri FreeRTOS în loc de trei thread-uri Java:
//
//   task periodic --(coada de alarme)--> task sporadic
//        |                                     |
//        +---- telemetry ---> MQTT <--- alarm -+
//                              |
//                          commands --> command_received() --(coada de comenzi)--> task aperiodic ---> status
//
// Gata implementate (nu trebuie modificate):
//   config.h         setările: Wi-Fi, broker, STUDENT, valorile inițiale; DEVICE_ID
//                    se citește din adresa MAC a plăcii
//   connection.h/cpp Wi-Fi, ora exactă (SNTP), clientul MQTT, mqtt_publish(), epoch_ms()
//
// Așa cum este, schița se conectează la Wi-Fi și la broker și afișează pe
// Serial comenzile primite. Taskurile le scrieți voi: TODO 0 ... TODO 3.
// Exemplele de pornire sunt în laboratorul 0 din rt-labs-esp32c3; vezi
// exemple/README.md din folderul temei.
//
// Placa: ESP32C3 Dev Module, USB CDC On Boot: Enabled. Serial Monitor: 115200 baud.

#include "config.h"
#include "connection.h"

#include <freertos/FreeRTOS.h>
#include <freertos/queue.h>

struct AlarmEvent {
    int64_t timestamp;
    float temperature;
    float threshold;
};

struct CommandEvent {
    char text[COMMAND_MAX_LEN];
};

QueueHandle_t alarmQueue = nullptr;
QueueHandle_t commandQueue = nullptr;

// Temperatura cipului, în °C, de la senzorul intern al ESP32-C3.
// Este temperatura siliciului, nu a camerei: imediat după pornirea Wi-Fi-ului
// poate fi în jur de 50 °C, apoi scade în câteva minute spre 35-40 °C.
// Rezoluția este de aproximativ 1 °C. Ca să provocați alarme, puneți pragul
// cu SET_THRESHOLD cu un grad sub valoarea citită.
float read_temperature(void) {
    return temperatureRead();
}

// TODO 1: TASK PERIODIC, cu perioada T (inițial PERIOD_MS_INITIAL).
//   Fiecare job:
//   - citește temperatura: read_temperature()
//   - publică telemetria: mqtt_publish("telemetry", "deviceId;timestamp;temperatura")
//     (timestamp = epoch_ms(), tipărit cu %lld; temperatura cu "%.2f")
//   - dacă temperatura depășește pragul (inițial THRESHOLD_INITIAL), pune
//     un eveniment în coada de alarme, fără să se blocheze (timeout 0).
//     Taskul periodic NU tratează alarma: o tratează taskul sporadic.
//   Eliberările respectă grila r_k = r_0 + kT: folosiți așteptare absolutăRESHOLD_INITI
//   (xTaskDelayUntil), nu relativă (delay, vTaskDelay), ca să nu apară derivă.
//   Perioada și pragul se schimbă prin comenzi (taskul aperiodic), deci sunt
//   variabile globale volatile, citite la fiecare job.
volatile uint32_t periodMs = PERIOD_MS_INITIAL;
volatile float thresholdC = THRESHOLD_INITIAL;

void periodicTask(void* parameter) {
    TickType_t lastWake = xTaskGetTickCount();

    for (;;) {
        float temperature = read_temperature();
        Serial.printf("[periodic] Job pornit, temperatura %.2f C\n", temperature);
        int64_t timestamp = epoch_ms();
        float threshold = thresholdC;

        char message[100];
        snprintf(message, sizeof(message), "%s;%lld;%.2f",
                 DEVICE_ID, (long long)timestamp, temperature);
        mqtt_publish("telemetry", message);

        if (temperature > threshold) {
            AlarmEvent alarm = {timestamp, temperature, threshold};
            xQueueSend(alarmQueue, &alarm, 0);
        }

        xTaskDelayUntil(&lastWake, pdMS_TO_TICKS(periodMs));
    }
}
// TODO 2: TASK SPORADIC, tratează alarmele de temperatură.
//   - așteaptă un eveniment din coada de alarme
//   - dacă de la ultima alarmă publicată au trecut mai puțin de
//     MIN_INTERARRIVAL_MS, respinge evenimentul (afișați un mesaj pe Serial)
//   - altfel publică: mqtt_publish("alarm", "deviceId;timestamp;temperatura;prag")
//   Pentru intervale folosiți un timp monoton, millis(), nu epoch_ms().
void sporadicTask(void* parameter) {
    AlarmEvent alarm;
    bool hasPublished = false;
    uint32_t lastAlarmMs = 0;

    for (;;) {
        if (xQueueReceive(alarmQueue, &alarm, portMAX_DELAY) != pdTRUE) {
            continue;
        }

        uint32_t now = millis();
        if (hasPublished && now - lastAlarmMs < MIN_INTERARRIVAL_MS) {
            Serial.println("[alarm] Eveniment respins: prea devreme.");
            continue;
        }

        char message[128];
        snprintf(message, sizeof(message), "%s;%lld;%.2f;%.2f",
                 DEVICE_ID, (long long)alarm.timestamp,
                 alarm.temperature, alarm.threshold);

        if (mqtt_publish("alarm", message)) {
            lastAlarmMs = millis();
            hasPublished = true;
        }
    }
}

// TODO 3: TASK APERIODIC, execută comenzile primite de la business service.
//   Comenzile ajung în command_received() (mai jos), care le pune în coada de
//   comenzi; fiecare comandă eliberează un job al taskului:
//     SET_INTERVAL <ms>       schimbă perioada T a taskului periodic
//     SET_THRESHOLD <valoare> schimbă pragul de alarmă
//     RESET                   readuce perioada și pragul la valorile inițiale
//     STATUS                  raportează perioada și pragul curente
//   După fiecare comandă publică o confirmare:
//     mqtt_publish("status", "deviceId;timestamp;OK SET_INTERVAL 1000")
//   O comandă necunoscută sau un parametru invalid nu oprește taskul: se
//   publică "ERROR <motiv>".

// Apelată de clientul MQTT pentru fiecare comandă primită (vezi connection.h).
void command_received(const char* text) {
    CommandEvent command;
    snprintf(command.text, sizeof(command.text), "%s", text);

    if (xQueueSend(commandQueue, &command, 0) == pdTRUE) {
        Serial.printf("[mqtt] Comanda pusa in coada: %s\n", command.text);
    } else {
        Serial.println("[mqtt] Coada de comenzi este plina.");
    }
}

void commandTask(void* parameter) {
    CommandEvent command;

    for (;;) {
        if (xQueueReceive(commandQueue, &command, portMAX_DELAY) != pdTRUE) {
            continue;
        }

        char reply[96];
        char* context = nullptr;
        const char* separators = " \t\r\n";
        char* name = strtok_r(command.text, separators, &context);
        char* argument = strtok_r(nullptr, separators, &context);
        char* extra = strtok_r(nullptr, separators, &context);

        if (name == nullptr) {
            snprintf(reply, sizeof(reply), "ERROR comanda goala");

        } else if (strcmp(name, "STATUS") == 0 && argument == nullptr) {
            snprintf(reply, sizeof(reply), "OK STATUS interval_ms=%lu prag=%.2f",
                     (unsigned long)periodMs, (double)thresholdC);

        } else if (strcmp(name, "RESET") == 0 && argument == nullptr) {
            periodMs = PERIOD_MS_INITIAL;
            thresholdC = THRESHOLD_INITIAL;
            snprintf(reply, sizeof(reply), "OK RESET interval_ms=%lu prag=%.2f",
                     (unsigned long)periodMs, (double)thresholdC);

        } else if (strcmp(name, "SET_INTERVAL") == 0) {
            char* end = nullptr;
            errno = 0;
            long value = argument ? strtol(argument, &end, 10) : 0;

            if (argument && !extra && end != argument && *end == '\0' &&
                errno == 0 && value > 0 && pdMS_TO_TICKS(value) > 0) {
                periodMs = (uint32_t)value;
                snprintf(reply, sizeof(reply), "OK SET_INTERVAL %ld", value);
            } else {
                snprintf(reply, sizeof(reply), "ERROR interval invalid");
            }

        } else if (strcmp(name, "SET_THRESHOLD") == 0) {
            char* end = nullptr;
            errno = 0;
            float value = argument ? strtof(argument, &end) : 0.0f;

            if (argument && !extra && end != argument && *end == '\0' &&
                errno == 0 && isfinite(value)) {
                thresholdC = value;
                snprintf(reply, sizeof(reply), "OK SET_THRESHOLD %.2f", (double)value);
            } else {
                snprintf(reply, sizeof(reply), "ERROR prag invalid");
            }

        } else {
            snprintf(reply, sizeof(reply), "ERROR comanda sau parametri invalizi");
        }

        char message[160];
        snprintf(message, sizeof(message), "%s;%lld;%s",
                 DEVICE_ID, (long long)epoch_ms(), reply);
        mqtt_publish("status", message);
        Serial.printf("[comanda] %s\n", reply);
    }
}


void setup() {
    Serial.begin(115200);
    // Fără așteptare la scriere: cu placa la USB, dar fără Serial Monitor deschis,
    // fiecare Serial.printf() ar aștepta până la 2 s să fie citit și ar întârzia
    // taskul care scrie. Așa, textul pe care nu-l citește nimeni se pierde.
    Serial.setTxTimeoutMs(0);
    uint32_t t = millis();
    while (!Serial && millis() - t < 3000) delay(10);  // așteptăm Serial Monitor, cel mult 3 s
    delay(200);
    Serial.println();
    Serial.printf("=== tema 02: STUDENT %s, DEVICE_ID %s ===\n", STUDENT, DEVICE_ID);

    wifi_connect();
    time_sync();

    // TODO 0 (a): creați cele două cozi (xQueueCreate): coada de alarme și
    //   coada de comenzi. Înainte de mqtt_start(): de acolo încolo pot sosi
    //   comenzi, iar command_received() le pune în coada de comenzi.
    alarmQueue = xQueueCreate(8, sizeof(AlarmEvent));
    commandQueue = xQueueCreate(8, sizeof(CommandEvent));

    if (alarmQueue == nullptr || commandQueue == nullptr) {
        Serial.println("Eroare la crearea cozilor.");
        return;
    }

    Serial.println("TODO 0(a): cozile sunt pregatite.");
    mqtt_start();
        xTaskCreate(periodicTask, "periodic", 4096, nullptr, 3, nullptr);
         xTaskCreate(sporadicTask, "sporadic", 4096, nullptr, 4, nullptr);
             xTaskCreate(commandTask, "commands", 4096, nullptr, 3, nullptr);

    // TODO 0 (b): porniți cele trei taskuri (xTaskCreate). După mqtt_start(),
    //   ca primele mesaje publicate să nu se piardă. Alegeți prioritățile
    //   între 2 și 19: peste loopTask (1), sub taskurile sistemului pentru
    //   evenimente și Wi-Fi (20, 23). Stiva, în octeți: cel puțin 4096.
}

void loop() {
    // Totul se face în taskuri: loop() nu are nimic de făcut.
    delay(1000);
}
