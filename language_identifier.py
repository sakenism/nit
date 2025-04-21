import fasttext
import os

# Download the language identification model if not already present
model_path = "lid.176.bin"
if not os.path.exists(model_path):
    print(f"Downloading FastText language identification model to {model_path}")
    import urllib.request
    urllib.request.urlretrieve("https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin", model_path)

# Load the pre-trained model
model = fasttext.load_model(model_path)

def identify_language(text):
    # Get predictions with probabilities
    predictions = model.predict(text, k=3)  # Get top 3 predictions
    
    # Extract results
    languages = [lang.replace("__label__", "") for lang in predictions[0]]
    probabilities = predictions[1]
    
    # Return results
    results = []
    result = None
    for lang, prob in zip(languages, probabilities):
        results.append((lang, prob))
        if prob > 0.3 and lang in ('kk', 'ky', 'tt'):
            result = 'kk'
    if result is None:
        result = results[0][0]
    
    return result

# Example usage
text_samples = [
    "Hello, how are you doing today?",
    "Bonjour, comment allez-vous aujourd'hui?",
    "Hola, ¿cómo estás hoy?",
    "Guten Tag, wie geht es Ihnen heute?",
    "こんにちは、今日はどうですか？",
    "удостоверение личности қалай алсам болады?",
    "мартта удостоверение личности алдым, дата срок действия қашан бітеді?",
    "Кешке барға барамыз ба, или дома останемся?",
    "Сенің телефоның қайда, не могу найти.",
    "Мектеп бүгін болмай ма, почему не сказали?",
    "Какой фильм көргіміз келеді бүгін?",
    "Сен жұмыс істеп жүрсің бе, или отпусктасың?",
    "Ана жаңа кафеге бардың ба, говорят вкусно там?",
    "Почему сен кеше звондамадың?",
    "Кешке не істейміз, может киноға барайық?",
    "Сенің атың кім, если не секрет?",
    "Бұл кімнің сумкасы, ты не видел?",
]

for text in text_samples:
    result = None
    print(f"\nText: {text}")
    try:
        result = identify_language(text)
    except Exception as err:
        print('Error', err)
    print(result)