# 1. Adım: Playwright'ın resmi, tüm bağımlılıkları içeren imajını kullan
# Bu, apt-get ve --with-deps hatalarını tamamen çözer.
FROM mcr.microsoft.com/playwright/python:v1.46.0-jammy

# 2. Adım: Uygulama dosyalarının bulunacağı bir çalışma dizini oluştur
WORKDIR /app

# 3. Adım: Önce sadece requirements.txt dosyasını kopyala
# Bu, Docker'ın katman önbelleğini daha verimli kullanmasını sağlar
COPY requirements.txt .

# 4. Adım: Python kütüphanelerini yükle
RUN pip install --no-cache-dir -r requirements.txt

# 5. Adım: Projenin geri kalan tüm dosyalarını kopyala
COPY . .

# 6. Adım: Uygulamanın dış dünyaya açılacağı portu belirt
EXPOSE 8080

# 7. Adım: Uygulamayı başlat (DİNAMİK PORT KULLANILACAK ŞEKİLDE GÜNCELLENDİ)
# Google Cloud Run tarafından sağlanan $PORT değişkenini kullanır.
# Yerelde çalıştırırken varsayılan olarak 8000 kullanır.
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}

