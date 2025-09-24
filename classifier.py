# classifier_stub.py
import random
from typing import List, Tuple

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F
class SentimentAnalyzer:
    def __init__(self, LevelSentiment = 0.5): # Порог эмоциональной оценки.
        #self.model_name = "cointegrated/rubert-tiny2"
        self.LevelSentiment = LevelSentiment
        self.model_name = "sentiment_model"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model.to(self.device)
        self.model.eval()

    def analyze_sentiment(self, text):
        """Анализирует эмоциональную окраску текста"""
        try:
            inputs = self.tokenizer(
                text,
                return_tensors='pt',
                truncation=True,
                padding=True,
                max_length=512
            ).to(self.device)

            with torch.no_grad():
                outputs = self.model(**inputs)
                probabilities = F.softmax(outputs.logits, dim=-1)
                sentiment_score = probabilities[0][1].item()  # 1 - позитивный класс

            # Определяем эмоциональную оценку
            if sentiment_score > self.LevelSentiment:
                return "Позитивный", sentiment_score
            else:
                return "Негативный", sentiment_score

        except Exception as e:
            print(f"Ошибка при анализе текста: {e}")
            return "Ошибка", 0.5




class SentimentClassifierStub:
    """
    Заглушка для классификатора — возвращает случайные или эвристические результаты.
    Нужна, чтобы протестировать систему без обучения модели.
    """

    def __init__(self):
        # Ничего не грузим — просто заглушка
        self.Analyzer = SentimentAnalyzer()

    def predict_in_batches(self, texts: List[str], batch_size: int = 8) -> Tuple[List[str], List[float]]:
        """
        Возвращает случайные метки и уверенности.
        В продакшене заменить на настоящую модель.
        """

        labels = []
        confidences = []

        for text in texts:
            # Простая эвристика: если есть "хорош", "отличн", "люб" — positive
            text_lower = text.lower()
            label, confidence = self.Analyzer.analyze_sentiment(text_lower)


            labels.append(label)
            confidences.append(confidence)

        return labels, confidences

if __name__ == "__main__":
    Classifier = SentimentClassifierStub()
    Commentary = ['Ну так, все под стать времени, че не так-то?', 'Да уж... Делают они, а стыдно мне.','Крутой фильм','Какое прекрасное качество']
    labels, conf = Classifier.predict_in_batches(Commentary)
    print(f"comm:{Commentary}\n"
          f"labels:{labels}\n"
          f"conf:{conf}")

