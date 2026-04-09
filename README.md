# 🏙️ SmartCity Enerji Pipeline'ı
### Microsoft Fabric | PySpark | Delta Lake | Power BI

> **"Elektriği ne zaman kullanmalıyım?"**
> Bu proje, Microsoft Fabric kullanarak uçtan uca bir Büyük Veri Pipeline'ı inşa ederek bu soruyu yanıtlıyor.

---

## 📌 Proje Hakkında

Bu proje bir **Büyük Veri Mühendisliği Pipeline'ı**dır.
3 farklı API'den gerçek zamanlı veri toplanır, temizlenir, analiz edilir ve Power BI'da görselleştirilir.

**Ana iş sorusu:**
> *"Şu an elektrik kullanmak için uygun mu? Fiyat ucuz VE rüzgar enerjisi mevcut mu?"*

---

## 🗺️ Proje Mimarisi

```
API'ler (EnergyZero, Open-Meteo, Air Quality)
         ↓
    Data Factory Pipeline
         ↓
   BRONZ KATMAN (Ham JSON)
         ↓
   GÜMÜŞ KATMAN (Temiz Delta Tabloları)
         ↓
   ALTIN KATMAN (Analiz + FIRSAT Etiketi)
         ↓
   Power BI Dashboard
```

---

## 🚀 Adım Adım Proje Yapımı

---

### ✅ ADIM 1 — Workspace Oluşturma

**Ne yaptık?**
Microsoft Fabric'te yeni bir Workspace oluşturduk.

**Neden yaptık?**
Workspace, projedeki tüm bileşenlerin (Lakehouse, Pipeline, Notebook'lar) bir arada tutulduğu **ana klasör** gibidir.
Tıpkı bir bilgisayarda proje klasörü oluşturmak gibi — her şey düzenli ve bir arada olur.

```
app.fabric.microsoft.com → New Workspace → SmartCity_Workspaces
```

---

### ✅ ADIM 2 — Lakehouse Oluşturma

**Ne yaptık?**
Workspace içinde `SmartCity_Lakehouse` adında bir Lakehouse oluşturduk.
Ardından Files klasörü altında şu yapıyı oluşturduk:

```
Files/
  ├── bronze/
  │     ├── energy/
  │     ├── weather/
  │     └── air_quality/
  ├── silver/
  └── gold/
```

**Neden yaptık?**
Lakehouse, **hem dosya hem de tablo** depolayabilen bir yapıdır.
- `bronze/` → API'den gelen ham JSON dosyaları buraya kaydedilir
- `silver/` → Temizlenmiş Delta tabloları buraya kaydedilir
- `gold/` → Analiz edilmiş ve raporlamaya hazır veriler buraya kaydedilir

Bu yapıya **Madalyon Mimarisi** (Medallion Architecture) denir.
Her katman bir öncekinden daha temiz ve daha değerli veri içerir.


> 💡 Neden Warehouse değil Lakehouse?
> Lakehouse hem dosya (JSON) hem de tablo (Delta) tutabilir.
> Bronz katmanda ham JSON dosyaları var, bu yüzden Lakehouse tercih ettik.

---

### ✅ ADIM 3 — Data Factory Pipeline Kurulumu

**Ne yaptık?**
`SmartCity_Pipeline` adında bir Data Factory Pipeline oluşturduk.
3 farklı API'ye bağlanan **Copy Data** aktiviteleri ekledik:

| Aktivite | API | Hedef |
|----------|-----|-------|
| CopyEnergyData | api.energyzero.nl | bronze/energy/energy_raw.json |
| CopyWeatherData | api.open-meteo.com | bronze/weather/weather_raw.json |
| CopyAirQualityData | air-quality-api.open-meteo.com | bronze/air_quality/air_quality_raw.json |

**Neden yaptık?**
API'lerden veriyi manuel olarak indirip yüklemek yerine, Pipeline bunu **otomatik** olarak yapar.
Her çalıştırıldığında güncel veriyi çekip Bronz katmana kaydeder.
Bu işleme **veri alımı (data ingestion)** denir.

**Kullanılan API'ler:**

```
1. Enerji Fiyatları:
https://api.energyzero.nl/v1/energyprices?fromDate=2024-12-01T00:00:00.000Z
&tillDate=2024-12-02T23:59:59.000Z&usageType=1&interval=4

2. Hava Durumu:
https://api.open-meteo.com/v1/forecast?latitude=52.3676&longitude=4.9041
&hourly=temperature_2m,wind_speed_10m,direct_radiation&past_days=7

3. Hava Kalitesi:
https://air-quality-api.open-meteo.com/v1/air-quality?latitude=52.3676
&longitude=4.9041&hourly=carbon_monoxide,nitrogen_dioxide,pm10&past_days=7
```

Pipeline çalıştırıldıktan sonra Bronz katmanda 3 ham JSON dosyası oluştu:
- ✅ `bronze/energy/energy_raw.json`
- ✅ `bronze/weather/weather_raw.json`
- ✅ `bronze/air_quality/air_quality_raw.json`

---

### ✅ ADIM 4 — 5 Notebook Oluşturma

**Ne yaptık?**
Projenin beyni olan 5 Notebook'u oluşturduk ve her birini SmartCity_Lakehouse'a bağladık.

**Neden 5 ayrı notebook?**
Her notebook'un **tek bir görevi** vardır. Bu yaklaşıma **Modüler Programlama** denir.
Bir notebook'ta hata olursa sadece onu düzeltirsin, diğerleri etkilenmez.

```
nb_config                   → Yapılandırma (Beyin)
nb_logging                  → Loglama (Kara Kutu)
nb_functions                → Araç Kutusu (7 Fonksiyon)
nb_generic_bronze_to_silver → ETL Motoru (Dönüşüm)
nb_process_silver_to_gold   → Analiz Motoru (Altın)
```

---

#### 📓 NOTEBOOK 1: `nb_config` — Yapılandırma

**Ne yaptık?**
Projenin tüm sabit değerlerini (dosya yolları, isimler) tek bir yerde tanımladık.

```python
LAKEHOUSE_NAME = "SmartCity_Lakehouse"
ROOT_PATH      = f"abfss://{LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com/..."
PATH_BRONZE    = f"{ROOT_PATH}/bronze"
PATH_SILVER    = f"{ROOT_PATH}/silver"
PATH_GOLD      = f"{ROOT_PATH}/gold"
FILE_FORMAT    = "json"
FULL_LOAD      = True

print("✅ Config Loaded")
```

**Neden yaptık?**
Eğer yolları her notebook'a ayrı ayrı yazsaydık:
- Lakehouse ismi değiştiğinde **50 farklı yerde** güncelleme yapmak gerekirdi
- Hata yapma ihtimali çok yüksek olurdu

`nb_config` sayesinde değişiklik gerekirse **sadece bu dosyayı** güncelleriz.
Diğer tüm notebook'lar `%run nb_config` komutuyla bu değişkenleri otomatik yükler.

Buna yazılımda **"Tek Doğruluk Kaynağı"** ve **DRY Prensibi** (Don't Repeat Yourself) denir.

---

#### 📓 NOTEBOOK 2: `nb_logging` — Loglama Sistemi

**Ne yaptık?**
Python'un `logging` kütüphanesiyle profesyonel bir loglama sistemi kurduk.

```python
def get_logger(name):
    logger = logging.getLogger(name)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
```

**Örnek çıktı:**
```
2026-04-08 21:08:16 - __main__ - INFO - ==== ENERGY işleniyor ====
2026-04-08 21:08:19 - __main__ - INFO - ENERGY kayıt sayısı: 48
2026-04-08 21:08:32 - __main__ - INFO - ==== ENERGY tamamlandı ====
```

**Neden yaptık?**
`print()` kullanmak büyük projelerde yetersizdir:
- Ekran kapanınca **kaybolur**
- Hangi mesajın hata, hangi mesajın normal bilgi olduğu **belli olmaz**
- **Tarih ve saat** bilgisi içermez

Loglama sistemi ise:
- Her mesajı **tarih/saat** ile kaydeder
- Mesajları **seviyelerine göre** ayırır (INFO, WARNING, ERROR)
- Tıpkı uçağın **kara kutusu** gibi çalışır — hata olursa tam olarak ne zaman, nerede olduğunu görebilirsin

| Seviye | Ne zaman kullanılır |
|--------|---------------------|
| DEBUG 🔍 | Geliştirme aşamasında detaylı bilgi |
| INFO ✅ | Her şey normal |
| WARNING ⚠️ | Dikkat gerektiren durum |
| ERROR ❌ | Bir şeyler yanlış gitti |

---

#### 📓 NOTEBOOK 3: `nb_functions` — Araç Kutusu

**Ne yaptık?**
Projede tekrar tekrar kullanılacak 7 fonksiyon yazdık.

**Neden yaptık?**
Aynı kodu 3 farklı veri seti için kopyala-yapıştır yapmak yerine,
bir kez fonksiyon yazıp her yerde çağırdık.
Hata olursa **tek bir yerden** düzeltilir.

Buna **DRY Prensibi** denir (Don't Repeat Yourself — Kendini Tekrarlama).

**7 Fonksiyon:**

---

**1. `clean_column_names(df)`**
Sütun adlarını snake_case formatına dönüştürür.
```
"Wind Speed (km/h)" → "wind_speed_km_h"
"Price.EUR"         → "price_eur"
"First Name"        → "first_name"
```
*Neden?* API'lerden gelen veriler karmaşık sütun adlarına sahip olabilir.
Standart bir format olmadan analiz yapmak zorlaşır.

---

**2. `explode_and_flatten(df, liste_sutunu)`**
İç içe geçmiş JSON'u düz tabloya çevirir.
```
# Önce (iç içe JSON):
{"prices": [{"readingDate": "2024-01-01", "price": 0.5}]}

# Sonra (düz tablo):
| readingDate | price |
| 2024-01-01  | 0.5   |
```
*Neden?* API'den gelen veriler iç içe kutular gibidir. Analiz için düz tablo gerekir.

---

**3. `save_to_lakehouse(df, table_name)`**
DataFrame'i Delta formatında Lakehouse'a kaydeder.
```python
df.write.format("delta").mode("overwrite").saveAsTable(table_name)
```
*Neden?* Her seferinde format, mod, yol gibi detayları düşünmek yerine
bu fonksiyonu çağırıyoruz. Fonksiyon her şeyi halleder.

---

**4. `generate_surrogate_key(df, sutunlar)`**
MD5 hash ile her satır için benzersiz ID üretir.
```
timestamp = "2024-01-01T00:00"
surrogate_key = md5("2024-01-01T00:00") = "a3f9b2c1..."
```
*Neden?* Büyük tablolarda metin eşleştirmek yavaştır.
Hash değerleri çok daha hızlı eşleşir.

---

**5. `create_time_series_frame(df, timestamp_col)`**
Zaman sütununu düzenler ve zamana göre sıralar.
```python
df = df.withColumn(timestamp_col, F.to_timestamp(F.col(timestamp_col)))
df = df.orderBy(timestamp_col)
```
*Neden?* Pencere fonksiyonları ve join işlemleri için
timestamp'lerin aynı formatta ve sıralı olması şarttır.

---

**6. `select_columns_safe(df, sutunlar)`**
Sütun yoksa hata vermek yerine atlar.
```python
# Normalde hata verir:
df.select("wind_direction")  # ❌ sütun yok → çöker!

# Güvenli seçici:
select_columns_safe(df, ["wind_direction"])  # ✅ uyarı verir, devam eder
```
*Neden?* API'den gelen veriler her zaman aynı sütunları içermez.
Kod çökmeden devam etmeli.

---

**7. `join_dataframes(df1, df2, anahtar)`**
İki tabloyu ortak sütun üzerinden birleştirir.
```python
df_gold = join_dataframes(df_energy, df_weather, "timestamp", "left")
```
*Neden?* Energy ve Weather tabloları ayrı ayrıdır.
"Rüzgar eserken fiyat neydi?" sorusunu cevaplamak için birleştirmek gerekir.

---

#### 📓 NOTEBOOK 4: `nb_generic_bronze_to_silver` — ETL Motoru

**Ne yaptık?**
Ham JSON verilerini temiz Delta tablolarına dönüştürdük.

**Neden bu şekilde yaptık? — Kapsül Mantığı**

Klasik yaklaşım:
```python
# Energy için ayrı kod
df_energy = spark.read.json("bronze/energy")
...

# Weather için ayrı kod
df_weather = spark.read.json("bronze/weather")
...

# Air Quality için ayrı kod — 3 kere aynı şeyi yazıyoruz!
```

Bizim yaklaşımımız (Kapsül Mantığı):
```python
# Kapsüller — sadece kurallar değişir
capsules = {
    "energy": {
        "source" : "Files/bronze/energy/energy_raw.json",
        "table"  : "silver_energy",
        "explode": "prices"
    },
    "weather": {
        "source" : "Files/bronze/weather/weather_raw.json",
        "table"  : "silver_weather",
        "zip"    : {"parent": "hourly", "children": ["time", "temperature_2m", ...]}
    },
    "air_quality": {
        "source" : "Files/bronze/air_quality/air_quality_raw.json",
        "table"  : "silver_air_quality",
        "zip"    : {"parent": "hourly", "children": ["time", "carbon_monoxide", ...]}
    }
}

# Motor — tek döngü her şeyi işler
for name, cap in capsules.items():
    if name == "energy":
        process_energy(cap["source"], cap["table"])
    else:
        process_zip(cap["source"], cap["table"], ...)
```

Tıpkı çamaşır makinesi gibi:
- **Makine (motor)** hep aynı
- **Program (kapsül)** değişiyor → farklı sonuç

Yarın yeni bir veri seti eklemek istersen sadece yeni kapsül eklersin, motoru değiştirmezsin! 🚀

**Bronz → Gümüş Dönüşümü Sonuçları:**

| Tablo | Kayıt Sayısı |
|-------|-------------|
| silver_energy | 48 kayıt |
| silver_weather | 336 kayıt |
| silver_air_quality | 288 kayıt |

---

#### 📓 NOTEBOOK 5: `nb_process_silver_to_gold` — Analiz Motoru

**Ne yaptık?**
3 Gümüş tabloyu birleştirdik, analiz ettik ve Altın katmana kaydettik.

**Adımlar:**

**Adım 1 — Tabloları oku ve timestamp formatını eşitle**
```python
df_energy      = spark.read.format("delta").table("silver_energy")
df_weather     = spark.read.format("delta").table("silver_weather")
df_air_quality = spark.read.format("delta").table("silver_air_quality")

# Timestamp formatları farklıydı, eşitledik:
# Energy:      "2024-12-01T00:00:00Z"
# Weather:     "2024-12-01T00:00"
# Hepsini →   "2024-12-01T00:00" yaptık
df_energy = df_energy.withColumn("timestamp", F.substring("timestamp", 1, 16))
```
*Neden?* Join işlemi için timestamp'lerin birebir eşleşmesi lazım.
Format farklı olursa eşleşme olmaz ve tüm satırlar NULL gelir!

---

**Adım 2 — Tabloları birleştir (Left Join)**
```python
df_gold = join_dataframes(df_energy, df_weather, "timestamp", "left")
df_gold = join_dataframes(df_gold, df_air_quality, "timestamp", "left")
```
*Neden Left Join?*
Energy ana tablomuz. Left join sayesinde Energy'deki tüm satırlar korunur.
Weather veya AirQuality'de eşleşme yoksa NULL gelir ama Energy satırı kaybolmaz.

---

**Adım 3 — 3 saatlik hareketli ortalama**
```python
window  = Window.orderBy("timestamp").rowsBetween(-3, 0)
df_gold = df_gold.withColumn("avg_price_3h", F.avg("price").over(window))
```
*Neden?* Sadece anlık fiyata bakmak yetmez.
"Şu an fiyat ucuz mu?" sorusunu cevaplamak için geçmişe bakmak gerekir.
Son 3 saatin ortalamasından düşükse → ucuzdur.

---

**Adım 4 — FIRSAT etiketi**
```python
df_gold = df_gold.withColumn(
    "opportunity",
    F.when(
        (F.col("price") < F.col("avg_price_3h")) &
        (F.col("wind_speed_10m") > 20),
        "FIRSAT"
    ).otherwise("NORMAL")
)
```
*Neden?* Şirketin asıl sorusunu cevaplıyoruz:
- ✅ Fiyat son 3 saatin ortalamasından düşük → Ucuz!
- ✅ Rüzgar hızı 20'den yüksek → Yenilenebilir enerji var!
- İkisi de sağlanıyorsa → **FIRSAT!**

---

**Altın Tablo Sonucu:**

| timestamp | price | wind_speed | avg_price_3h | opportunity |
|-----------|-------|------------|--------------|-------------|
| 2024-12-01T00:00 | 0.05 | 32.2 | 0.08 | **FIRSAT** ✅ |
| 2024-12-01T01:00 | 0.08 | 28.4 | 0.065 | NORMAL |
| 2024-12-01T02:00 | 0.10 | 15.0 | 0.077 | NORMAL |

---

### ✅ ADIM 5 — Power BI Dashboard

**Ne yaptık?**
`gold_energy_analysis` tablosunu Power BI'a bağladık ve 3 görsel oluşturduk.

**Neden yaptık?**
Veri mühendisliğinin son adımı verileri **görselleştirmektir**.
Yöneticiler ve iş kullanıcıları ham tabloları okuyamaz.
Power BI sayesinde veriler anlamlı grafiklere dönüşür ve kararlar kolayca alınabilir.

**Bağlantı yöntemi: Direct Lake**
Veri kopyalanmaz — Power BI direkt Lakehouse'daki tabloya bağlanır.
Bu sayede veri her zaman güncel kalır.

**Oluşturulan Görseller:**
- 📈 **Fiyat Trendi** — Zaman içinde elektrik fiyatlarının değişimi (Çizgi Grafik)
- 📊 **FIRSAT / NORMAL Dağılımı** — Kaç saat FIRSAT vardı? (Çubuk Grafik)
- 🌬️ **Rüzgar & Fiyat Korelasyonu** — Rüzgar arttıkça fiyat düşüyor mu? (Dağılım Grafiği)

---

## 📈 Proje Sonuçları

| Katman | Tablo | Kayıt |
|--------|-------|-------|
| Bronz | energy_raw.json | Ham veri |
| Bronz | weather_raw.json | Ham veri |
| Bronz | air_quality_raw.json | Ham veri |
| Gümüş | silver_energy | 48 |
| Gümüş | silver_weather | 336 |
| Gümüş | silver_air_quality | 288 |
| Altın | gold_energy_analysis | 48 (birleştirilmiş) |

---

## 🧠 Öğrenilen Temel Kavramlar

| Kavram | Açıklama |
|--------|----------|
| **Madalyon Mimarisi** | Bronz → Gümüş → Altın katman yapısı |
| **Metadata-Driven ETL** | Kapsül mantığı ile tek motor, çoklu veri seti |
| **DRY Prensibi** | Kendini tekrarlama — fonksiyonlar ve config |
| **Delta Lake** | Güvenilir, versiyonlanmış veri depolama |
| **Pencere Fonksiyonları** | PySpark'ta hareketli ortalama hesaplama |
| **Modüler Kod** | Config / Logging / Functions ayrımı |
| **Left Join** | Ana tabloyu koruyarak diğer tabloları ekleme |
| **Direct Lake** | Power BI'ın veriyi kopyalamadan okuması |

---

## ⚙️ Kullanılan Teknolojiler

| Teknoloji | Amaç |
|-----------|------|
| Microsoft Fabric | Bulut veri platformu |
| Apache Spark (PySpark) | Büyük veri işleme motoru |
| Delta Lake | Güvenilir tablo depolama |
| Data Factory Pipeline | API'den otomatik veri çekme |
| Power BI | Dashboard ve görselleştirme |
| Python | Notebook programlama dili |

---

'''
##EKSTRA NOTLAR 
Delta Tabloları Nasıl Oluşturduk?
save_to_lakehouse fonksiyonu ile!
nb_functions notebook'unda yazdığımız bu fonksiyon sayesinde:
pythondef save_to_lakehouse(df, table_name, mode="overwrite"):
    df.write.format("delta").mode(mode).saveAsTable(table_name)
    print(f"✅ Kaydedildi: {table_name}")
Bu fonksiyon 3 şey yapıyor:
1. format("delta") → Veriyi Delta formatında kaydet
Delta, normal Parquet'ten farklı olarak:

Değişiklik geçmişi tutar
Üzerine yazabilirsin
Hata olursa geri alabilirsin

2. mode("overwrite") → Tablo zaten varsa üzerine yaz
3. saveAsTable(table_name) → Lakehouse'un Tables klasörüne kaydet, böylece SQL ile sorgulanabilir hale gelir
Ne zaman çağırdık?

nb_generic_bronze_to_silver → silver_energy, silver_weather, silver_air_quality tablolarını oluştururken
nb_process_silver_to_gold → gold_energy_analysis tablosunu oluştururken

Kısaca:
DataFrame → .write.format("delta").saveAsTable() → Tables klasörü ✅
Neden Delta Tabloları Oluşturduk?
1. Power BI'a Bağlanmak İçin
Delta tabloları Lakehouse'un Tables klasöründe görünür. Power BI direkt buradan okuyabilir. Eğer dosya olarak kaydetsek (Files klasörü) Power BI göremezdi.
2. SQL ile Sorgulanabilir
Delta tablo olunca şöyle sorgulayabilirsin:
sqlSELECT * FROM gold_energy_analysis
WHERE opportunity = 'FIRSAT'
Dosya olarak kalsaydı bunu yapamazdın.

##Bronz → Gümüş → Altın Mantığı
Her katmanda veriyi bir üst seviyeye taşıdık:
bronze (ham JSON dosyası)
    ↓ temizlendi
silver (Delta tablo — okunabilir)
    ↓ analiz edildi
gold (Delta tablo — karar verilebilir)
4. Güvenilir ve Hızlı
Delta formatı:

Hızlı okur → Spark optimizasyonu
Güvenli → Hata olursa önceki versiyona dönebilirsin
Tutarlı → Veri bozulmaz

Kısaca:

Ham JSON dosyaları analiz için uygun değil. Delta tablolarına dönüştürünce hem temiz, hem hızlı, hem de Power BI'a bağlanabilir hale geldi

## nb_config — Neden Oluşturduk?
Temel Problem: Sabit Kodlama
Düşün, eğer her notebook'a şöyle yazsaydık:
pythondf = spark.read.json("Files/bronze/energy/energy_raw.json")
Ve yarın Lakehouse ismini değiştirmek istesek, 50 farklı yerde bu satırı bulmak ve değiştirmek zorunda kalırdık. Bu hem zaman kaybı hem de hata riski demek.
Çözüm: Tek Doğruluk Kaynağı
nb_config tüm sabit değerleri tek bir yerde toplar. Değişiklik gerekirse sadece burayı güncelliyoruz, diğer tüm notebook'lar otomatik düzeliyor.

Koddaki Her Satır Ne İşe Yarıyor?
pythonLAKEHOUSE_NAME = "SmartCity_Lakehouse"
👉 Lakehouse'un adını bir değişkene atadık. İleride isim değişirse sadece burayı güncelleriz.

pythonROOT_PATH = f"abfss://{LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}.Lakehouse/Files"
👉 Lakehouse'un tam adresi — GPS koordinatı gibi. abfss:// Microsoft'un bulut depolama adresi formatıdır.

pythonPATH_BRONZE = f"{ROOT_PATH}/bronze"
PATH_SILVER = f"{ROOT_PATH}/silver"
PATH_GOLD   = f"{ROOT_PATH}/gold"
👉 Her katmanın yolunu ayrı değişkenlere atadık. Diğer notebook'larda PATH_BRONZE yazınca otomatik olarak doğru yola gider.

python_SOURCE = "smart_city"
👉 Projenin kaynak adı. Loglarda ve tablolarda hangi projeden geldiğini anlamak için kullanılır.

pythonFILE_FORMAT = "json"
👉 Bronze katmandaki dosyaların formatı JSON. Bunu değişkene atayınca ileride format değişirse sadece burayı güncelleriz.

pythonFULL_LOAD = True
👉 Veriyi tamamen yeniden mi yükleyelim, yoksa sadece yeni gelenleri mi ekleyelim? True = her seferinde tamamen yeniden yükle.

pythonprint("✅ Config Loaded")
👉 Notebook çalışınca bize "hazır" sinyali verir. Veri işleme yapmaz, sadece değişkenleri tanımlar.

Diğer Notebook'larla Bağlantısı
nb_config
    ↓ %run nb_config
nb_generic_bronze_to_silver → PATH_BRONZE kullanır
nb_process_silver_to_gold   → PATH_SILVER, PATH_GOLD kullanır
%run nb_config komutu sayesinde diğer notebook'lar bu değişkenleri otomatik olarak hafızaya alır. 🙂

Kısaca:

nb_config projenin ana sigorta kutusu. Tüm adresler ve sabit değerler burada. Değişiklik gerekirse tek bir yerden hallederiz.

## nb_logging — Neden Oluşturduk?
Temel Problem: print() Yetmez
Birçok kişi kodda hata ararken şöyle yapar:
pythonprint("Veri okundu")
print("İşlem tamamlandı")
Küçük projelerde bu yeterli. Ama büyük projelerde sorunlar çıkar:

Ekran kapanınca print çıktısı kaybolur
Hangi mesajın hata, hangi mesajın normal bilgi olduğu belli olmaz
Tarih ve saat bilgisi olmaz — ne zaman hata olduğunu bilemezsin

Çözüm: Logging Sistemi
nb_logging tüm mesajları seviyelerine göre ayırır ve tarih/saat bilgisiyle birlikte gösterir.

Koddaki Her Satır Ne İşe Yarıyor?
pythonimport logging
👉 Python'un yerleşik logging kütüphanesini içe aktardık. Ekstra kurulum gerektirmez.

pythondef get_logger(name: str = __name__) -> logging.Logger:
👉 Logger oluşturan bir fonksiyon tanımladık. name parametresi hangi notebook'tan çağrıldığını gösterir. Örneğin nb_generic_bronze_to_silver'dan çağrılırsa orası görünür.

pythonif logger.handlers:
    return logger
👉 Logger zaten oluşturulduysa tekrar oluşturma. Aksi halde her çağrıda mesajlar iki kez yazılırdı.

pythonlogger.setLevel(logging.DEBUG)
👉 En düşük seviyeyi DEBUG olarak ayarladık. Yani DEBUG, INFO, WARNING, ERROR — hepsini göster.

pythonformatter = logging.Formatter(
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
👉 Mesajların formatını belirledik. Çıktı şöyle görünür:
2026-04-08 21:08:16 - __main__ - INFO - Veri okundu

%(asctime)s → Tarih ve saat
%(name)s → Hangi notebook
%(levelname)s → Seviye (INFO, WARNING, ERROR)
%(message)s → Mesajın kendisi


pythonhandler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
👉 Mesajları konsola (ekrana) yazdırmak için handler ekledik ve formatı uyguladık.

pythonlogger.info("✅ Logging sistemi başlatıldı")
logger.debug("🔍 Debug mesajı")
logger.warning("⚠️ Uyarı mesajı")
logger.error("❌ Hata mesajı")
👉 4 farklı seviyeyi test ettik:
SeviyeNe zaman kullanılır?DEBUGGeliştirme aşamasında detaylı bilgiINFOHer şey normal, bilgi amaçlıWARNINGÇalışıyor ama dikkat etERRORBir şeyler yanlış gitti

Diğer Notebook'larla Bağlantısı
python# nb_generic_bronze_to_silver içinde
logger = get_logger(__name__)
logger.info("==== ENERGY işleniyor ====")
logger.info("ENERGY kayıt sayısı: 48")
%run nb_logging komutuyla diğer notebook'lar get_logger() fonksiyonunu kullanabildi. Projede gördüğümüz şu çıktılar hep bu sayede oluştu:
2026-04-08 21:08:16 - __main__ - INFO - ==== ENERGY işleniyor ====
2026-04-08 21:08:19 - __main__ - INFO - ENERGY kayıt sayısı: 48
2026-04-08 21:08:32 - __main__ - INFO - ==== ENERGY tamamlandı ====

Kısaca:

nb_logging projenin kara kutusu. Kod çalışırken ne olduğunu tarih/saat bilgisiyle kaydeder. Hata olursa tam olarak ne zaman ve nerede olduğunu görebilirsin

## nb_functions — Neden Oluşturduk?
Temel Problem: Tekrar Eden Kod
Düşün, her veri seti için aynı işlemi tekrar tekrar yazmak zorunda kalsaydık:
python# Energy için
df_energy = df_energy.withColumnRenamed("First Name", "first_name")
df_energy = df_energy.withColumnRenamed("Wind Speed", "wind_speed")

# Weather için
df_weather = df_weather.withColumnRenamed("First Name", "first_name")
df_weather = df_weather.withColumnRenamed("Wind Speed", "wind_speed")

# Air Quality için
df_air_quality = df_air_quality.withColumnRenamed("First Name", "first_name")
...
Bu hem zaman kaybı hem de hata riski. Bir yerde hata bulursan 3 farklı yerde düzeltmen lazım.
Çözüm: DRY Prensibi
Don't Repeat Yourself — Kendini Tekrarlama!
Bir kez fonksiyon yaz, her yerde çağır. 🙂

7 Fonksiyonun Her Biri Ne İşe Yarıyor?

1. clean_column_names(df)
pythondef clean_column_names(df):
    for col in df.columns:
        new_col = col.strip().lower()
        new_col = re.sub(r'[^a-z0-9]+', '_', new_col)
        new_col = new_col.strip('_')
        df = df.withColumnRenamed(col, new_col)
    return df
👉 Sütun adlarını temizler ve snake_case yapar:
"First Name"     → "first_name"
"Wind Speed(km)" → "wind_speed_km"
"Price.EUR"      → "price_eur"

strip().lower() → Baştaki/sondaki boşlukları sil, küçük harfe çevir
re.sub(r'[^a-z0-9]+', '_', ...) → Harf ve rakam olmayan her şeyi _ ile değiştir
strip('_') → Baştaki/sondaki _ işaretlerini sil


2. explode_and_flatten(df, liste_sutunu)
pythondef explode_and_flatten(df, liste_sutunu):
    df = df.withColumn(liste_sutunu, F.explode(F.col(liste_sutunu)))
    fields = df.schema[liste_sutunu].dataType.fields
    for field in fields:
        df = df.withColumn(field.name, F.col(f"{liste_sutunu}.{field.name}"))
    df = df.drop(liste_sutunu)
    return df
👉 İç içe geçmiş JSON'u düz tabloya çevirir:
# Önce (iç içe):
{"prices": [{"readingDate": "2024-01-01", "price": 0.5}]}

# Sonra (düz tablo):
| readingDate | price |
| 2024-01-01  | 0.5   |

F.explode() → Listeyi satırlara açar
fields → İç içe yapının sütunlarını bulur
drop() → Artık gereksiz olan iç içe sütunu siler


3. save_to_lakehouse(df, table_name, mode)
pythondef save_to_lakehouse(df, table_name, mode="overwrite"):
    df.write.format("delta").mode(mode).saveAsTable(table_name)
    print(f"✅ Kaydedildi: {table_name}")
👉 DataFrame'i Delta tablosu olarak Lakehouse'a kaydeder:

format("delta") → Delta formatında kaydet
mode("overwrite") → Tablo varsa üzerine yaz
saveAsTable() → Tables klasörüne kaydet, SQL ile sorgulanabilir olsun


4. generate_surrogate_key(df, sutunlar)
pythondef generate_surrogate_key(df, sutunlar):
    df = df.withColumn(
        "surrogate_key",
        F.md5(F.concat_ws("_", *[F.col(s).cast(StringType()) for s in sutunlar]))
    )
    return df
👉 Her satır için benzersiz bir ID üretir:
timestamp = "2024-01-01T00:00"
surrogate_key = md5("2024-01-01T00:00") = "a3f9b2c1..."

concat_ws("_", ...) → Sütunları _ ile birleştirir
F.md5() → MD5 hash algoritmasıyla benzersiz ID üretir

Neden gerekli?
Büyük tablolarda metin eşleştirmek yavaştır. Hash değerleri çok daha hızlı eşleşir.

5. create_time_series_frame(df, timestamp_col)
pythondef create_time_series_frame(df, timestamp_col):
    df = df.withColumn(timestamp_col, F.to_timestamp(F.col(timestamp_col)))
    df = df.orderBy(timestamp_col)
    return df
👉 Zaman sütununu düzenler:

F.to_timestamp() → Metni gerçek tarih/saat formatına çevirir
orderBy() → Zamana göre sıralar

Neden gerekli?
Pencere fonksiyonları ve join işlemleri için timestamp'lerin aynı formatta olması şart.

6. select_columns_safe(df, sutunlar)
pythondef select_columns_safe(df, sutunlar):
    mevcut = [s for s in sutunlar if s in df.columns]
    eksik  = [s for s in sutunlar if s not in df.columns]
    if eksik:
        print(f"⚠️ Eksik sütunlar atlandı: {eksik}")
    return df.select(mevcut)
👉 Sütun yoksa hata vermek yerine atlar:
# Normalde hata verir:
df.select("wind_direction")  # ❌ sütun yok → hata!

# Güvenli seçici ile:
select_columns_safe(df, ["wind_direction"])  # ✅ uyarı verir, devam eder
Neden gerekli?
API'den gelen veriler her zaman aynı sütunları içermez. Kod çökmeden devam etmeli.

7. join_dataframes(df1, df2, anahtar, nasil)
pythondef join_dataframes(df1, df2, anahtar, nasil="left"):
    df = df1.join(df2, on=anahtar, how=nasil)
    return df
👉 İki tabloyu ortak bir sütun üzerinden birleştirir:
Energy tablosu + Weather tablosu → timestamp üzerinden birleştir

| timestamp | price | temperature | wind_speed |
| 2024-01-01| 0.5   | 7.6         | 32.2       |

on=anahtar → Hangi sütun üzerinden birleştirilecek
how="left" → Sol tablo (Energy) ana tablo, sağdan (Weather) ekle


Diğer Notebook'larla Bağlantısı
nb_functions
    ↓ %run nb_functions
nb_generic_bronze_to_silver → clean_column_names, generate_surrogate_key, save_to_lakehouse kullandı
nb_process_silver_to_gold   → join_dataframes, generate_surrogate_key, save_to_lakehouse kullandı

Kısaca:

nb_functions projenin araç kutusu. Tekrar eden işleri fonksiyonlara dönüştürdük. Bir kez yaz, her yerde kullan. Hata olursa tek bir yerden düzelt.

##nb_generic_bronze_to_silver — Neden Oluşturduk?
Temel Problem: Her Veri İçin Ayrı Kod
Klasik bir programcı şöyle düşünür:
python# Energy için kod
df_energy = spark.read.json("bronze/energy")
df_energy = df_energy.explode(...)
df_energy.write.save("silver/energy")

# Weather için ayrı kod
df_weather = spark.read.json("bronze/weather")
df_weather = df_weather.explode(...)
df_weather.write.save("silver/weather")

# Air Quality için ayrı kod
df_air = spark.read.json("bronze/air_quality")
...
Sorun: Yarın yeni bir veri seti gelirse yeni kod yazmak lazım. Bir yerde hata olursa 3 ayrı yerde düzeltmek lazım.
Çözüm: Kapsül Mantığı (Metadata-Driven ETL)
Bir veri mühendisi şöyle düşünür:

"Tek bir motor yazacağım, kurallara göre çalışacak."

Tıpkı çamaşır makinesi gibi:

Makine (motor) hep aynı
Program (kapsül) değişiyor → farklı sonuç


Koddaki Her Bölüm Ne İşe Yarıyor?

Bölüm 1: Diğer Notebook'ları Yükle
python%run nb_config
%run nb_logging
%run nb_functions
👉 Bu 3 komut şunu yapar:

nb_config → PATH_BRONZE, PATH_SILVER gibi değişkenleri yükler
nb_logging → get_logger() fonksiyonunu hazır hale getirir
nb_functions → 7 fonksiyonu kullanıma açar


Bölüm 2: Logger Başlat
pythonlogger = get_logger(__name__)
👉 Bu notebook için özel bir logger oluşturur. Tüm mesajlar tarih/saat ile birlikte görünür.

Bölüm 3: Kapsül Tanımları
pythoncapsules = {
    "energy": {
        "source"  : "Files/bronze/energy/energy_raw.json",
        "table"   : "silver_energy",
        "explode" : "prices",
        "columns" : ["readingDate", "price"]
    },
    "weather": {
        "source"  : "Files/bronze/weather/weather_raw.json",
        "table"   : "silver_weather",
        "zip"     : {"parent": "hourly", "children": ["time", "temperature_2m", "wind_speed_10m", "direct_radiation"]},
        "columns" : ["time", "temperature_2m", "wind_speed_10m", "direct_radiation"]
    },
    "air_quality": {
        "source"  : "Files/bronze/air_quality/air_quality_raw.json",
        "table"   : "silver_air_quality",
        "zip"     : {"parent": "hourly", "children": ["time", "carbon_monoxide", "nitrogen_dioxide", "pm10"]},
        "columns" : ["time", "carbon_monoxide", "nitrogen_dioxide", "pm10"]
    }
}
👉 Her veri seti için kurallar burada tanımlandı:
AlanNe anlama geliyor?sourceBronze'daki ham dosyanın yolutableSilver'da oluşturulacak tablonun adıexplodeHangi sütun patlatılacak (Energy için)zipHangi sütunlar birleştirileceek (Weather/AirQuality için)columnsHangi sütunlar seçilecek
Kapsülün gücü: Yarın yeni bir veri seti gelirse sadece buraya yeni bir kapsül eklersin. Motor kodunu değiştirmene gerek yok! 🙂

Bölüm 4: process_energy Fonksiyonu
pythondef process_energy(source, table):
    logger.info(f"==== ENERGY işleniyor ====")
    
    # 1. Bronze'dan oku
    df = spark.read.option("multiline", "true").json(source)
    
    # 2. prices listesini patlat
    df = df.withColumn("prices", F.explode(F.col("prices")))
    
    # 3. İç içe yapıdan sütunları çıkar
    df = df.select(
        F.col("prices.readingDate").alias("timestamp"),
        F.col("prices.price").alias("price")
    )
    
    # 4. Timestamp formatını düzelt
    df = df.withColumn("timestamp", F.substring(F.col("timestamp"), 1, 16))
    
    # 5. Sütun adlarını temizle
    df = clean_column_names(df)
    
    # 6. Surrogate key ekle
    df = generate_surrogate_key(df, ["timestamp"])
    
    # 7. Zaman serisi çerçevesi oluştur
    df = create_time_series_frame(df, "timestamp")
    
    # 8. Silver'a kaydet
    save_to_lakehouse(df, table)
👉 Energy verisi özel bir yapıda geldi. prices adlı bir liste içinde her saatin fiyatı vardı. Bu yüzden önce listeyi patlattık, sonra düzleştirdik.
Ham Energy JSON:
json{
  "prices": [
    {"readingDate": "2024-12-01T00:00:00Z", "price": 0.05},
    {"readingDate": "2024-12-01T01:00:00Z", "price": 0.08}
  ]
}
Silver Energy tablosu:
| timestamp       | price |
| 2024-12-01T00:00| 0.05  |
| 2024-12-01T01:00| 0.08  |

Bölüm 5: process_zip Fonksiyonu
pythondef process_zip(source, table, parent, children):
    logger.info(f"==== {table.upper()} işleniyor ====")
    
    # 1. Bronze'dan oku
    df = spark.read.option("multiline", "true").json(source)
    
    # 2. Her sütunu ayrı ayrı patlat
    dfs = []
    for c in children:
        tmp = df.select(F.explode(F.col(f"{parent}.{c}")).alias(c))
        tmp = tmp.withColumn("idx", F.monotonically_increasing_id())
        dfs.append(tmp)
    
    # 3. idx üzerinden birleştir
    result = dfs[0]
    for d in dfs[1:]:
        result = result.join(d, "idx")
    result = result.drop("idx")
    
    # 4. Sütun adlarını düzenle ve kaydet
    result = result.withColumnRenamed("time", "timestamp")
    result = clean_column_names(result)
    result = generate_surrogate_key(result, ["timestamp"])
    result = create_time_series_frame(result, "timestamp")
    save_to_lakehouse(result, table)
👉 Weather ve AirQuality verileri farklı bir yapıda geldi. hourly adlı bir obje içinde her sütun ayrı bir liste olarak vardı.
Ham Weather JSON:
json{
  "hourly": {
    "time": ["2024-12-01T00:00", "2024-12-01T01:00"],
    "temperature_2m": [7.6, 7.2],
    "wind_speed_10m": [32.2, 28.4]
  }
}
Silver Weather tablosu:
| timestamp        | temperature_2m | wind_speed_10m |
| 2024-12-01T00:00 | 7.6            | 32.2           |
| 2024-12-01T01:00 | 7.2            | 28.4           |
zip mantığı şunu yapıyor:

Her listeyi ayrı ayrı patlat
idx (sıra numarası) ile hizala
Yan yana birleştir


Bölüm 6: Motor Döngüsü
pythonfor name, cap in capsules.items():
    if name == "energy":
        process_energy(cap["source"], cap["table"])
    else:
        process_zip(
            cap["source"],
            cap["table"],
            cap["zip"]["parent"],
            cap["zip"]["children"]
        )
👉 Bu döngü tüm kapsülleri sırayla işler:

energy → process_energy() çağırır
weather → process_zip() çağırır
air_quality → process_zip() çağırır

Kapsülün gücü burada görünür: Yarın yeni bir veri seti eklemek istersen sadece capsules sözlüğüne yeni bir kapsül eklersin, döngü otomatik işler!

Sonuçta Ne Elde Ettik?
Bronze (ham JSON)
    ├── energy_raw.json      → silver_energy (48 kayıt)
    ├── weather_raw.json     → silver_weather (336 kayıt)
    └── air_quality_raw.json → silver_air_quality (288 kayıt)

Kısaca:

nb_generic_bronze_to_silver projenin dönüşüm motoru. Ham JSON verilerini temiz Delta tablolarına çevirir. Kapsül mantığı sayesinde yeni veri seti eklemek çok kolay — sadece yeni kapsül tanımla, motor otomatik işler! 

## nb_process_silver_to_gold — Neden Oluşturduk?
Temel Problem: Veriler Ayrı Tablolarda
Silver katmanında 3 temiz tablo var ama hepsi ayrı ayrı:
silver_energy      → timestamp, price
silver_weather     → timestamp, temperature, wind_speed
silver_air_quality → timestamp, carbon_monoxide, nitrogen_dioxide, pm10
Şirketin sorusu şu:

"Rüzgar eserken elektrik fiyatı ucuz muydu? Ne zaman enerji kullanmalıyım?"

Bu soruyu cevaplamak için 3 tabloyu birleştirmek ve analiz etmek lazım. İşte bu notebook tam olarak bunu yapıyor!

Koddaki Her Bölüm Ne İşe Yarıyor?

Bölüm 1: Diğer Notebook'ları Yükle
python%run nb_config
%run nb_logging
%run nb_functions
👉 Tıpkı önceki notebook gibi:

nb_config → PATH_SILVER, PATH_GOLD yollarını yükler
nb_logging → get_logger() fonksiyonunu hazır hale getirir
nb_functions → join_dataframes, save_to_lakehouse gibi fonksiyonları açar


Bölüm 2: Logger Başlat
pythonlogger = get_logger(__name__)
👉 Bu notebook için özel logger oluşturur. Tüm adımlar tarih/saat ile loglanır.

Bölüm 3: Silver Tablolardan Oku
pythondf_energy      = spark.read.format("delta").table("silver_energy")
df_weather     = spark.read.format("delta").table("silver_weather")
df_air_quality = spark.read.format("delta").table("silver_air_quality")
👉 Silver katmandaki 3 Delta tablosunu okuduk. format("delta") diyoruz çünkü tablolar Delta formatında kaydedilmişti.
pythondf_energy      = df_energy.drop("surrogate_key")
df_weather     = df_weather.drop("surrogate_key")
df_air_quality = df_air_quality.drop("surrogate_key")
👉 Her tabloda surrogate_key sütunu vardı. Join edince 3 tane surrogate_key çakışırdı. Bu yüzden önce düşürdük.

Bölüm 4: Timestamp Formatını Eşitle
pythondf_energy      = df_energy.withColumn("timestamp", F.substring(F.col("timestamp"), 1, 16))
df_weather     = df_weather.withColumn("timestamp", F.substring(F.col("timestamp"), 1, 16))
df_air_quality = df_air_quality.withColumn("timestamp", F.substring(F.col("timestamp"), 1, 16))
👉 3 tablodaki timestamp formatları farklıydı:
Energy      → "2024-12-01T00:00:00Z"
Weather     → "2024-12-01T00:00"
Air Quality → "2024-12-01T00:00"
substring(..., 1, 16) ile hepsini aynı formata getirdik:
"2024-12-01T00:00"
Neden önemli? Join işlemi için timestamp'lerin birebir eşleşmesi lazım. Format farklı olursa eşleşme olmaz ve tüm satırlar NULL gelir!

Bölüm 5: Tabloları Birleştir
pythondf_gold = join_dataframes(df_energy, df_weather, "timestamp", "left")
df_gold = join_dataframes(df_gold, df_air_quality, "timestamp", "left")
👉 3 tabloyu timestamp üzerinden birleştirdik:
Adım 1: Energy + Weather
| timestamp        | price | temperature | wind_speed |
| 2024-12-01T00:00 | 0.05  | 7.6         | 32.2       |
| 2024-12-01T01:00 | 0.08  | 7.2         | 28.4       |
Adım 2: + Air Quality
| timestamp        | price | temperature | wind_speed | carbon_monoxide | pm10 |
| 2024-12-01T00:00 | 0.05  | 7.6         | 32.2       | 245.0           | 12.3 |
| 2024-12-01T01:00 | 0.08  | 7.2         | 28.4       | 230.0           | 11.8 |
Neden Left Join?
Energy ana tablomuz. Left join sayesinde Energy'deki tüm satırlar korunur. Weather veya AirQuality'de eşleşme yoksa NULL gelir ama Energy satırı kaybolmaz.

Bölüm 6: 3 Saatlik Hareketli Ortalama
pythonwindow = Window.orderBy("timestamp").rowsBetween(-3, 0)
df_gold = df_gold.withColumn("avg_price_3h", F.avg("price").over(window))
👉 Her satır için son 3 saatin ortalama fiyatını hesapladık:
| timestamp        | price | avg_price_3h |
| 2024-12-01T00:00 | 0.05  | 0.05         |
| 2024-12-01T01:00 | 0.08  | 0.065        |
| 2024-12-01T02:00 | 0.03  | 0.053        |
| 2024-12-01T03:00 | 0.10  | 0.065        |

Window.orderBy("timestamp") → Zamana göre sırala
rowsBetween(-3, 0) → Şu anki satır dahil geriye 3 satıra bak
F.avg("price").over(window) → O penceredeki fiyatların ortalamasını al

Neden gerekli?
Sadece anlık fiyata bakmak yeterli değil. Geçmişe kıyasla ucuz mu pahalı mı olduğunu anlamak için ortalama lazım.

Bölüm 7: FIRSAT Etiketi
pythondf_gold = df_gold.withColumn(
    "opportunity",
    F.when(
        (F.col("price") < F.col("avg_price_3h")) &
        (F.col("wind_speed_10m") > 20),
        "FIRSAT"
    ).otherwise("NORMAL")
)
👉 Her satıra FIRSAT veya NORMAL etiketi ekledik:
FIRSAT koşulları:

price < avg_price_3h → Şu anki fiyat son 3 saatin ortalamasından düşük
wind_speed_10m > 20 → Rüzgar hızı 20 km/h'den yüksek (yenilenebilir enerji var)

İkisi de sağlanıyorsa → FIRSAT!
| timestamp        | price | avg_price_3h | wind_speed | opportunity |
| 2024-12-01T00:00 | 0.05  | 0.08         | 32.2       | FIRSAT      |
| 2024-12-01T01:00 | 0.08  | 0.065        | 28.4       | NORMAL      |
| 2024-12-01T02:00 | 0.10  | 0.077        | 15.0       | NORMAL      |

Bölüm 8: Gold Katmana Kaydet
pythondf_gold.show(10)
save_to_lakehouse(df_gold, "gold_energy_analysis")
👉 Sonucu ekranda gösterdik ve Gold katmana Delta tablosu olarak kaydettik.

Sonuçta Ne Elde Ettik?
Silver (temiz tablolar)
    ├── silver_energy
    ├── silver_weather
    └── silver_air_quality
           ↓ join + analiz
Gold (karar destek tablosu)
    └── gold_energy_analysis
         ├── timestamp
         ├── price
         ├── temperature_2m
         ├── wind_speed_10m
         ├── carbon_monoxide
         ├── nitrogen_dioxide
         ├── pm10
         ├── avg_price_3h
         └── opportunity (FIRSAT / NORMAL)

Kısaca:

nb_process_silver_to_gold projenin analiz motoru. 3 ayrı Silver tablosunu birleştirdi, pencere fonksiyonuyla geçmişe baktı ve şirkete "Şu an enerji kullanmak için FIRSAT mı?" sorusunu cevapladı. Sonucu Gold katmana kaydetti, Power BI'a hazır! 💡

'''

## 👤 Yazar

**Azizeke**
Veri Mühendisliği Projesi — Microsoft Fabric
Nisan 2026
