from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import pipeline, AutoTokenizer, AutoModelForQuestionAnswering

app = FastAPI()


from fastapi.middleware.cors import CORSMiddleware

# Habilitar CORS para todos los orígenes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cambia esto para restringir a dominios específicos si lo deseas
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos HTTP
    allow_headers=["*"],  # Permite todos los encabezados
)


# Cargar el modelo y el tokenizador manualmente
model_name = "bert-large-uncased-whole-word-masking-finetuned-squad"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForQuestionAnswering.from_pretrained(model_name)

# Usar la tarea correcta en el pipeline
qa_pipeline = pipeline("question-answering", model=model, tokenizer=tokenizer)

class Query(BaseModel):
    pregunta: str
    contexto: str

@app.get("/")
def home():
    return {"message": "¡Bienvenido a la API de Respuestas!"}

@app.post("/consulta/")
def responder_pregunta(query: Query):
    try:
        # Usar el pipeline para obtener la respuesta
        
        respuesta = qa_pipeline(question=query.pregunta, context=query.contexto)
        return {"respuesta": respuesta['answer'], "confianza": respuesta["score"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Esta función correrá el servidor
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)

