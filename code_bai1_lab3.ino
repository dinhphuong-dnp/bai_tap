#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <Adafruit_BMP085.h>
#include <ArduinoJson.h>
#include <math.h>
#include <string.h>

// ================= CẤU HÌNH PHẦN CỨNG =================
#define PIN_LED       18
#define PIN_BUZZER    19
#define I2C_SDA       21
#define I2C_SCL       22

// ================ CẤU HÌNH WIFI VÀ MQTT ================
const char* WIFI_SSID   = "Wokwi-GUEST";
const char* WIFI_PASS   = "";
const char* MQTT_BROKER = "broker.hivemq.com";
const int   MQTT_PORT   = 1883;

const char* DEVICE_ID = "dnpuiong"; ///mã thiết bị trên web

// Ngưỡng cảnh báo.
const float TEMP_HIGH = 40.0;
const float TEMP_LOW  = 20.0;
const float ALT_CHANGE_LIMIT = 3.0;
const float SEA_LEVEL_PRESSURE = 101325.0; // Pa

// MQTT topics.
char TOPIC_DATA[80];
char TOPIC_CMD_LED[80];
char TOPIC_CMD_BUZZER[80];

// ================= CẤU TRÚC DỮ LIỆU =================
struct SensorData {
  float temperature;
  float pressure;
  float altitude;
};

struct AlertData {
  float temperature;
  float altitude;
};

// ================= ĐỐI TƯỢNG VÀ HÀNG ĐỢI =================
Adafruit_BMP085 bmp;
WiFiClient espClient;
PubSubClient mqttClient(espClient);

QueueHandle_t dataQueue;
QueueHandle_t alertQueue;

SemaphoreHandle_t outputMutex;
SemaphoreHandle_t serialMutex;

bool manualLedState = false;
bool manualBuzzerState = false;
bool alarmState = false;

// Khai báo callback MQTT trước khi sử dụng.
void mqttCallback(char* topic, byte* payload, unsigned int length);

// Cập nhật LED và còi theo trạng thái manual hoặc cảnh báo.
void updateOutputs() {
  bool ledOn;
  bool buzzerOn;

  xSemaphoreTake(outputMutex, portMAX_DELAY);

  ledOn = manualLedState || alarmState;
  buzzerOn = manualBuzzerState || alarmState;

  digitalWrite(PIN_LED, ledOn ? HIGH : LOW);

  if (buzzerOn) {
    tone(PIN_BUZZER, 1000);
  } else {
    noTone(PIN_BUZZER);
  }

  xSemaphoreGive(outputMutex);
}

// ================= CALLBACK NHẬN LỆNH MQTT =================
void mqttCallback(char* topic, byte* payload, unsigned int length) {
  bool turnOn = (length == 2 && memcmp(payload, "ON", 2) == 0);
  bool turnOff = (length == 3 && memcmp(payload, "OFF", 3) == 0);

  if (!turnOn && !turnOff) {
    Serial.println("Lệnh không hợp lệ. Dùng ON hoặc OFF.");
    return;
  }

  xSemaphoreTake(outputMutex, portMAX_DELAY);

  if (strcmp(topic, TOPIC_CMD_LED) == 0) {
    manualLedState = turnOn;
  } else if (strcmp(topic, TOPIC_CMD_BUZZER) == 0) {
    manualBuzzerState = turnOn;
  }

  xSemaphoreGive(outputMutex);

  updateOutputs();
  Serial.printf("Đã nhận lệnh %s: %s\n",
                topic, turnOn ? "ON" : "OFF");
}

// ================= KẾT NỐI LẠI MQTT =================
void reconnectMQTT() {
  while (!mqttClient.connected()) {
    Serial.print("[MQTT] Đang kết nối Broker...");

    String clientId = "ESP32-Runner-" + String(random(0xffff), HEX);

    if (mqttClient.connect(clientId.c_str())) {
      Serial.println(" thành công!");
      mqttClient.subscribe(TOPIC_CMD_LED);
      mqttClient.subscribe(TOPIC_CMD_BUZZER);
      Serial.println("Đã đăng ký nhận lệnh LED và còi.");
    } else {
      Serial.printf(" thất bại, mã lỗi=%d. Thử lại sau 2 giây.\n",
                    mqttClient.state());
      vTaskDelay(pdMS_TO_TICKS(2000));
    }
  }
}

// ================= TASK 1: ĐỌC CẢM BIẾN (CORE 1) =================
void vTaskReadSensor(void* pvParameters) {
  (void)pvParameters;
  TickType_t lastWakeTime = xTaskGetTickCount();

  for (;;) {
    SensorData sensor;

    sensor.temperature = bmp.readTemperature();
    sensor.pressure = bmp.readPressure() / 100.0; // Pa đổi sang hPa
    sensor.altitude = bmp.readAltitude(SEA_LEVEL_PRESSURE);

    // In số đo theo dạng dễ theo dõi.
    xSemaphoreTake(serialMutex, portMAX_DELAY);
    Serial.println("------------------------------------");
    Serial.printf("Nhiệt độ: %.1f C | Áp suất: %.1f hPa | Độ cao: %.1f m\n",
                  sensor.temperature,
                  sensor.pressure,
                  sensor.altitude);
    xSemaphoreGive(serialMutex);

    // Gửi đầy đủ số liệu cho task MQTT.
    if (xQueueSend(dataQueue, &sensor, pdMS_TO_TICKS(50)) != pdPASS) {
      Serial.println("dataQueue đầy, bỏ mẫu đo này.");
    }

    // Gửi nhiệt độ và độ cao cho task cảnh báo.
    AlertData alert;
    alert.temperature = sensor.temperature;
    alert.altitude = sensor.altitude;
    xQueueOverwrite(alertQueue, &alert);

    vTaskDelayUntil(&lastWakeTime, pdMS_TO_TICKS(1000));
  }
}

// ================= TASK 2: XỬ LÝ CẢNH BÁO (CORE 1) =================
void vTaskAlert(void* pvParameters) {
  (void)pvParameters;

  AlertData alert;
  float previousAltitude = 0.0;
  bool isFirstRead = true;

  for (;;) {
    if (xQueueReceive(alertQueue, &alert, portMAX_DELAY) == pdPASS) {
      bool needAlert = false;

      // Cảnh báo nếu nhiệt độ trên 40°C hoặc dưới 20°C.
      bool temperatureAlarm =
        (alert.temperature > TEMP_HIGH || alert.temperature < TEMP_LOW);

      if (temperatureAlarm) {
        needAlert = true;
      }

      // Kiểm tra độ cao có thay đổi đột ngột không.
      bool altitudeAlarm = false;
      float altitudeChange = 0.0;

      if (!isFirstRead) {
        altitudeChange = fabsf(alert.altitude - previousAltitude);

        if (altitudeChange > ALT_CHANGE_LIMIT) {
          needAlert = true;
          altitudeAlarm = true;
        }
      }

      // In cảnh báo sau phần in số đo.
      if (temperatureAlarm || altitudeAlarm) {
        xSemaphoreTake(serialMutex, portMAX_DELAY);

        Serial.println(
          "-> CANH BAO: Nhiệt độ bất thường hoặc độ cao đổi đột ngột"
        );

        if (temperatureAlarm) {
          Serial.printf("   Nhiệt độ hiện tại: %.1f C\n",
                        alert.temperature);
        }

        if (altitudeAlarm) {
          Serial.printf("   Độ cao thay đổi: %.1f m\n",
                        altitudeChange);
        }

        xSemaphoreGive(serialMutex);
      }

      previousAltitude = alert.altitude;
      isFirstRead = false;

      xSemaphoreTake(outputMutex, portMAX_DELAY);
      alarmState = needAlert;
      xSemaphoreGive(outputMutex);

      updateOutputs();
    }
  }
}

// ================= TASK 3: WIFI VÀ MQTT (CORE 0) =================
void vTaskMqttWifi(void* pvParameters) {
  (void)pvParameters;

  Serial.println("[CORE 0] Đang kết nối WiFi...");

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS, 6);

  while (WiFi.status() != WL_CONNECTED) {
    vTaskDelay(pdMS_TO_TICKS(500));
    Serial.print(".");
  }

  Serial.printf("\nWiFi đã kết nối. IP: %s\n",
                WiFi.localIP().toString().c_str());

  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);

  SensorData receivedData;

  for (;;) {
    // Nếu WiFi bị ngắt, kết nối lại.
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("WiFi bị ngắt. Đang kết nối lại...");
      WiFi.begin(WIFI_SSID, WIFI_PASS, 6);

      while (WiFi.status() != WL_CONNECTED) {
        vTaskDelay(pdMS_TO_TICKS(500));
      }
    }

    // Kết nối lại MQTT nếu bị ngắt.
    if (!mqttClient.connected()) {
      reconnectMQTT();
    }

    mqttClient.loop();

    // Lấy dữ liệu từ queue rồi gửi lên Broker.
    if (xQueueReceive(dataQueue, &receivedData,
                      pdMS_TO_TICKS(50)) == pdPASS) {
      StaticJsonDocument<200> jsonDocument;

      jsonDocument["nhiệt độ"] = receivedData.temperature;
      jsonDocument["áp suất"] = receivedData.pressure;
      jsonDocument["độ cao"] = receivedData.altitude;

      char jsonBuffer[200];
      serializeJson(jsonDocument, jsonBuffer, sizeof(jsonBuffer));

      if (mqttClient.connected()) {
        mqttClient.publish(TOPIC_DATA, jsonBuffer);
        Serial.printf("MQTT TX Đã gửi: %s\n", jsonBuffer);
      }
    }

    vTaskDelay(pdMS_TO_TICKS(20));
  }
}

// ================= KHỞI TẠO HỆ THỐNG =================
void setup() {
  Serial.begin(115200);

  pinMode(PIN_LED, OUTPUT);
  pinMode(PIN_BUZZER, OUTPUT);
  digitalWrite(PIN_LED, LOW);
  noTone(PIN_BUZZER);

  Wire.begin(I2C_SDA, I2C_SCL);

  if (!bmp.begin()) {
    Serial.println(
      "LỖI: Không tìm thấy BMP180. Kiểm tra dây SDA/SCL và nguồn."
    );

    while (true) {
      delay(1000);
    }
  }

  // Tạo các topic MQTT riêng theo mã thiết bị.
  snprintf(TOPIC_DATA, sizeof(TOPIC_DATA),
           "runner/%s/telemetry", DEVICE_ID);
  snprintf(TOPIC_CMD_LED, sizeof(TOPIC_CMD_LED),
           "runner/%s/cmd/led", DEVICE_ID);
  snprintf(TOPIC_CMD_BUZZER, sizeof(TOPIC_CMD_BUZZER),
           "runner/%s/cmd/buzzer", DEVICE_ID);

  // Tạo queue và mutex.
  dataQueue = xQueueCreate(10, sizeof(SensorData));
  alertQueue = xQueueCreate(1, sizeof(AlertData));
  outputMutex = xSemaphoreCreateMutex();
  serialMutex = xSemaphoreCreateMutex();

  if (dataQueue == NULL ||
      alertQueue == NULL ||
      outputMutex == NULL ||
      serialMutex == NULL) {
    Serial.println("LỖI: Không tạo được Queue hoặc Mutex.");

    while (true) {
      delay(1000);
    }
  }

  // Tạo các task và ghim vào đúng Core.
  xTaskCreatePinnedToCore(
    vTaskMqttWifi,
    "TaskMqttWifi",
    8192,
    NULL,
    1,
    NULL,
    0
  );

  xTaskCreatePinnedToCore(
    vTaskReadSensor,
    "TaskReadSensor",
    4096,
    NULL,
    2,
    NULL,
    1
  );

  xTaskCreatePinnedToCore(
    vTaskAlert,
    "TaskAlert",
    3072,
    NULL,
    3,
    NULL,
    1
  );
}

void loop() {
  // Các hoạt động chính do ba task FreeRTOS thực hiện.
  vTaskDelete(NULL);
}