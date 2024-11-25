<img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>

# Funciones de IA en Databricks
Capacitación práctica en la plataforma Databricks con enfoque en funcionalidades de IA generativa y el uso de Databricks AI Functions

# Hands-On LAB - Análisis de sentimiento, extracción de entidades y generación de texto

Vamos a utilizar las [AI Functions](https://docs.databricks.com/en/large-language-models/ai-functions.html)

Estos nos permiten ejecutar modelos de IA generativa en nuestras bases de datos corporativas directamente en consultas SQL, un lenguaje ampliamente utilizado por analistas de datos y negocios. Con esto también podremos crear nuevas tablas con la información extraída para utilizarla en nuestros análisis.

Existen funciones nativas para realizar tareas predefinidas o enviar cualquier instrucción deseada para su ejecución. A continuación se muestran las descripciones:

| Gen AI SQL Function | Descrição |
| -- | -- |
| [ai_analyze_sentiment](https://docs.databricks.com/pt/sql/language-manual/functions/ai_analyze_sentiment.html) | Análisis de sentimiento |
| [ai_classify](https://docs.databricks.com/pt/sql/language-manual/functions/ai_classify.html) | Clasifica el texto según categorías definidas. |
| [ai_extract](https://docs.databricks.com/pt/sql/language-manual/functions/ai_extract.html) | Extraiga las entidades deseadas |
| [ai_fix_grammar](https://docs.databricks.com/pt/sql/language-manual/functions/ai_fix_grammar.html) | Corrige a gramática do texto fornecido |
| [ai_gen](https://docs.databricks.com/pt/sql/language-manual/functions/ai_gen.html) | Corrige la gramática del texto proporcionado. | 
| [ai_mask](https://docs.databricks.com/pt/sql/language-manual/functions/ai_mask.html) | Enmascarar datos confidenciales |
| [ai_query](https://docs.databricks.com/pt/sql/language-manual/functions/ai_query.html) | Enviar instrucciones para el modelo deseado. |
| [ai_similarity](https://docs.databricks.com/pt/sql/language-manual/functions/ai_similarity.html) | Calcula la similitud entre dos expresiones. |
| [ai_summarize](https://docs.databricks.com/pt/sql/language-manual/functions/ai_summarize.html) | Resume el texto dado. |
| [ai_translate](https://docs.databricks.com/pt/sql/language-manual/functions/ai_translate.html) | Traduce el texto proporcionado |

El cuaderno <code>1. IA Function</code> trae algunos ejemplos.




