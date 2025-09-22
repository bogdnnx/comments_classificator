# classifier_stub.py
import random
from typing import List, Tuple

class SentimentClassifierStub:
    """
    Заглушка для классификатора — возвращает случайные или эвристические результаты.
    Нужна, чтобы протестировать систему без обучения модели.
    """

    def __init__(self):
        # Ничего не грузим — просто заглушка
        pass

    def predict_in_batches(self, texts: List[str], batch_size: int = 64) -> Tuple[List[str], List[float]]:
        """
        Возвращает случайные метки и уверенности.
        В продакшене заменить на настоящую модель.
        """
        labels = []
        confidences = []

        for text in texts:
            # Простая эвристика: если есть "хорош", "отличн", "люб" — positive
            text_lower = text.lower()
            if any(word in text_lower for word in ["хорош", "отличн", "люблю", "круто", "рекомендую"]):
                label = "positive"
                confidence = round(random.uniform(0.7, 0.99), 2)
            elif any(word in text_lower for word in ["плохо", "отстой", "не нрав", "глючит", "брак"]):
                label = "negative"
                confidence = round(random.uniform(0.7, 0.99), 2)
            else:
                # Иначе — случайно
                label = random.choice(["positive", "negative"])
                confidence = round(random.uniform(0.5, 0.9), 2)

            labels.append(label)
            confidences.append(confidence)

        return labels, confidences