# classifier_stub.py
import random
from typing import List, Tuple

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F

class SentimentAnalyzer:
    def __init__(self, LevelSentiment = 0.5): # Порог эмоциональной оценки.
        self.model_name = "rubert-base-cased-sentiment"
        self.LevelSentiment = LevelSentiment
        #self.model_name = "sentiment_model"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model.to(self.device)
        self.model.eval()
        self.emoji_weight = 0.25
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

            positive_emojis = {
                    # Текстовые
                    ':)', ':-)', '=)', ':D', ':-D', '=D', ';)', ';-)', ':P', ':-P',
                    # Emoji
                    '😊', '🙂', '😄', '😃', '😁', '😇', '😍', '🥰', '🤗', '👍',
                    '❤️', '💖', '💕', '✨', '🎉', '🥳'
                }
            negative_emojis = {    ':(', ':-(', '=(', ':/', ':-/', ':\\', ':-\\', ':|', ':-|',
                                    # Emoji
                                    '😞', '😔', '😢', '😭', '😠', '😡', '😖', '😩', '😓', '👎',
                                    '💔', '🤢', '🤬'
                                   }

            has_positive_emoji = any(emoji in text for emoji in positive_emojis)
            has_negative_emoji = any(emoji in text for emoji in negative_emojis)
            #print(has_positive_emoji, has_negative_emoji)
            with torch.no_grad():
                outputs = self.model(**inputs)
                probabilities = F.softmax(outputs.logits, dim=-1)
                sentiment_score_neutral = probabilities[0][0].item() # нейтрал
                sentiment_score = probabilities[0][1].item()  + self.emoji_weight * has_positive_emoji# позитив
                sentiment_score_negative = probabilities[0][2].item() + self.emoji_weight * has_negative_emoji# негатив
                scores = [sentiment_score_neutral, sentiment_score, sentiment_score_negative]
            # Определяем эмоциональную оценку
            print(scores)
            max_index = scores.index(max(scores))
            labels = ["neutral", "positive", "negative"]
            label = labels[max_index]
            return label, scores[max_index]

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
            text_lower = text.lower()
            label, confidence = self.Analyzer.analyze_sentiment(text_lower)


            labels.append(label)
            confidences.append(confidence)

        return (labels, confidences)

if __name__ == "__main__":
    Classifier = SentimentClassifierStub()
    Commentary = ['Ну так, все под стать времени, че не так-то?', 'Да уж... Делают они, а стыдно мне.','Крутой фильм','Какое прекрасное качество']
    Result = Classifier.predict_in_batches(Commentary)
    print(type(Result))
    labels, conf = Result
    print(type(labels),type(conf))
    print(type(labels[0]),type(conf[0]))
    print(f"comm:{Commentary}\n"
          f"labels:{labels}\n"
          f"conf:{conf}")

