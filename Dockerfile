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
EXPOSE 8000

# 7. Adım: Uygulamayı başlat
# Not: Sunucunuzun dışarıdan gelen bağlantıları dinlemesi için --host 0.0.0.0 gereklidir.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
