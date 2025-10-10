#!/bin/bash

# ----- 1. Giriş ve proje tanımlama -----
gcloud auth login
gcloud config set project tennis-predictor

# ----- 2. API'leri etkinleştir -----
gcloud services enable run.googleapis.com cloudbuild.googleapis.com

# ----- 3. Docker imajını oluştur ve Google Container Registry'ye yükle -----
gcloud builds submit --tag gcr.io/$(gcloud config get-value project)/tennis-app

# ----- 4. Cloud Run servisini oluştur veya güncelle -----
gcloud run deploy tennis-app \
  --image gcr.io/$(gcloud config get-value project)/tennis-app \
  --platform managed \
  --region europe-west1 \
  --allow-unauthenticated \
  --memory 1Gi \
  --cpu 1

# ----- 5. Yayın adresini göster -----
echo ""
echo "✅ Uygulama başarıyla dağıtıldı!"
echo "🌐 Adres:"
gcloud run services describe tennis-app --region europe-west1 --format 'value(status.url)'
