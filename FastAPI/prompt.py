from google import genai
from pprint import pprint

api_key = "AIzaSyCwPnbsn8xlIfkbakm3hHN3I3rgTXx0TFU"

client = genai.Client(api_key = api_key)

response = client.models.generate_content(model = "gemini-2.5-flash",
                                          contents = "Explicame como funciona la IA en pocas palabras")

print(response.text)