-- Databricks notebook source
-- MAGIC %md <img src="https://github.com/mousastech/iafunciones/blob/a29291ca12bdaace778eb9dc8b70ee301cd9bf7e/img/headertools_aifunctions.png?raw=true" width=100%>
-- MAGIC
-- MAGIC # Análisis de sentimiento, extracción de entidades y generación de texto.
-- MAGIC
-- MAGIC Capacitación práctica en la plataforma Databricks con enfoque en funcionalidades de IA generativa.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Objetivos del ejercicio
-- MAGIC
-- MAGIC El objetivo de este laboratorio es implementar el siguiente caso de uso:
-- MAGIC
-- MAGIC ### Aumentar la satisfacción del cliente con el análisis automático de reseñas
-- MAGIC
-- MAGIC En este laboratorio, crearemos un canal de datos que toma las opiniones de los clientes, en formato de texto libre, y las enriquece con información extraída haciendo preguntas en lenguaje natural a los modelos de IA generativa disponibles en Databricks. También brindaremos recomendaciones para las siguientes mejores acciones a nuestro equipo de servicio al cliente, es decir, si un cliente requiere seguimiento y un borrador de mensaje de respuesta.
-- MAGIC
-- MAGIC Para cada evaluación, nosotros:
-- MAGIC
-- MAGIC - Identificamos el sentimiento del cliente y extraemos los productos mencionados.
-- MAGIC - Generamos una respuesta personalizada para el cliente
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review.png" width="100%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preparación
-- MAGIC
-- MAGIC Para realizar los ejercicios, necesitamos prende un Clúster.
-- MAGIC
-- MAGIC Simplemente siga los pasos a continuación:
-- MAGIC 1. En la esquina superior derecha, haga clic en **Conectar**
-- MAGIC 2. Seleccione el tipo de Clúster **SQL Serverless Warehouse** o **Serverless**.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Conjunto de datos de ejemplo
-- MAGIC
-- MAGIC Ahora, accedamos a las reseñas de productos que subimos en la práctica de laboratorio anterior.
-- MAGIC
-- MAGIC En esta práctica de laboratorio usaremos dos tablas:
-- MAGIC - **Evaluaciones**: datos no estructurados con el contenido de las evaluaciones
-- MAGIC - **Clientes**: datos estructurados como registro de clientes y consumo.
-- MAGIC
-- MAGIC ¡Ahora visualicemos estos datos!

-- COMMAND ----------

-- MAGIC %md ### A. Preparación de datos
-- MAGIC
-- MAGIC 1. Crear o utilizar el catalogo `tutorial`
-- MAGIC 2. Crear o utilizar el schema `carga`
-- MAGIC 3. Crear el volumen `archivos`
-- MAGIC 4. Importar los archivos de la carpeta `data` para el Volumen creado

-- COMMAND ----------

USE funcionesai.carga

-- COMMAND ----------

-- MAGIC %md ### B. Ver la tabla de calificaciones

-- COMMAND ----------

SELECT * FROM opiniones

-- COMMAND ----------

-- MAGIC %md ### C. Ver la tabla de clientes

-- COMMAND ----------

SELECT * FROM clientes

-- COMMAND ----------

-- MAGIC %md ## A. Analizar el sentimiento y extraer información.
-- MAGIC
-- MAGIC Nuestro objetivo es permitir un análisis rápido de grandes volúmenes de reseñas de forma rápida y eficiente. Para hacer esto, necesitamos extraer la siguiente información:
-- MAGIC
-- MAGIC - Productos mencionados
-- MAGIC - Sentimiento del cliente
-- MAGIC - En caso negativo, ¿cuál es el motivo de la insatisfacción?
-- MAGIC
-- MAGIC Veamos cómo podemos aplicar la IA generativa para acelerar nuestro trabajo.
-- MAGIC
-- MAGIC | Gen AI SQL Function | Descrição |
-- MAGIC | -- | -- |
-- MAGIC | [ai_analyze_sentiment](https://docs.databricks.com/pt/sql/language-manual/functions/ai_analyze_sentiment.html) | Análisis de sentimiento |
-- MAGIC | [ai_classify](https://docs.databricks.com/pt/sql/language-manual/functions/ai_classify.html) | Clasifica el texto según categorías definidas. |
-- MAGIC | [ai_extract](https://docs.databricks.com/pt/sql/language-manual/functions/ai_extract.html) | Extraiga las entidades deseadas |
-- MAGIC | [ai_fix_grammar](https://docs.databricks.com/pt/sql/language-manual/functions/ai_fix_grammar.html) | Corrige a gramática do texto fornecido |
-- MAGIC | [ai_gen](https://docs.databricks.com/pt/sql/language-manual/functions/ai_gen.html) | Corrige la gramática del texto proporcionado. | 
-- MAGIC | [ai_mask](https://docs.databricks.com/pt/sql/language-manual/functions/ai_mask.html) | Enmascarar datos confidenciales |
-- MAGIC | [ai_query](https://docs.databricks.com/pt/sql/language-manual/functions/ai_query.html) | Enviar instrucciones para el modelo deseado. |
-- MAGIC | [ai_similarity](https://docs.databricks.com/pt/sql/language-manual/functions/ai_similarity.html) | Calcula la similitud entre dos expresiones. |
-- MAGIC | [ai_summarize](https://docs.databricks.com/pt/sql/language-manual/functions/ai_summarize.html) | Resume el texto dado. |
-- MAGIC | [ai_translate](https://docs.databricks.com/pt/sql/language-manual/functions/ai_translate.html) | Traduce el texto proporcionado |

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### 🚀 Análisis de sentimiento

-- COMMAND ----------

SELECT  *, ai_analyze_sentiment(avaliacao) AS sentimento FROM opiniones LIMIT 10

-- COMMAND ----------

-- MAGIC %md #### 📝 Traducción

-- COMMAND ----------

-- DBTITLE 1,Ejemplo
SELECT ai_translate('Hello, how are you?', 'es') texto;

-- COMMAND ----------

-- DBTITLE 1,Uso de ai_translate en una función
SELECT  *, ai_translate(avaliacao, 'es') AS sentimento FROM opiniones LIMIT 10

-- COMMAND ----------

-- MAGIC %md #### 🔎 Extracción de los productos mencionados

-- COMMAND ----------

SELECT *, ai_extract(avaliacao, ARRAY('produto')) AS productos FROM opiniones LIMIT 10

-- COMMAND ----------

-- MAGIC %md ####  🔎 Extrayendo el motivo de la insatisfacción.
-- MAGIC
-- MAGIC *SUGERENCIA: Utilice la función AI_QUERY() para proporcionar un mensaje personalizado*

-- COMMAND ----------

SELECT *, ai_query(
  'databricks-mixtral-8x7b-instruct',
  concat('Si el sentimiento de evaluación es negativo, enumere los motivos de la insatisfacción. Evaluación: ', avaliacao)) AS motivo_insatisfaccion
FROM opiniones LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🔎 Analizar el sentimiento y extraer entidades a escala
-- MAGIC
-- MAGIC Tener que especificar las instrucciones varias veces acaba siendo laborioso, especialmente para los Analistas de Datos que deben centrarse en analizar los resultados de esta extracción.
-- MAGIC
-- MAGIC Para simplificar el acceso a esta inteligencia, crearemos una función SQL para encapsular este proceso y poder informar simplemente a qué columna de nuestro conjunto de datos nos gustaría aplicarlo.
-- MAGIC
-- MAGIC ¡Aquí, aprovechemos para enviar una única consulta a nuestro modelo para extraer toda la información de una vez!
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review-wrapper.png" width="1200px">

-- COMMAND ----------

-- MAGIC %md #### ✔️ Creando una función para extraer toda la información

-- COMMAND ----------

CREATE OR REPLACE FUNCTION revision_evaluacion(avaliacao STRING)
RETURNS STRUCT<produto_nome: STRING, produto_categoria: STRING, sentimento: STRING, resposta: STRING, resposta_motivo: STRING>
RETURN FROM_JSON(
  AI_QUERY(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT(
      'Un cliente dejó una reseña. Respondemos a cualquiera que parezca insatisfecho.
        Extraiga la siguiente información:
        - extraer el nombre del producto
        - extraer la categoría del producto, por ejemplo: tableta, portátil, teléfono inteligente
        - clasificar el sentimiento como ["POSITIVO","NEGATIVO","NEUTRAL"]
        - regresar si el sentimiento es NEGATIVO y necesita una respuesta: S o N
        - si el sentimiento es NEGATIVO, explica las razones principales
        Devuelve solo un JSON. Ningún otro texto que no sea JSON. Formato JSON:
      {
        "nombre_producto": <entidade nombre>,
        "producto_categoria": <entidade categoria>,
        "sentimiento": <entidade sentimiento>,
        "respuesta": <Sí o No para respuestas>,
        "motivo": <razones de insatisfacción>
      }
      Evaluación: ', avaliacao
    )
  ),
  "STRUCT<producto_categoria: STRING, producto_categoria: STRING, sentimiento: STRING, respuesta: STRING, motivo: STRING>"
)

-- COMMAND ----------

-- MAGIC %md #### ✔️ Análisis de revisión de pruebas

-- COMMAND ----------

SELECT revision_evaluacion('Compré una tableta Samsung y estoy muy descontento con la calidad de la batería. Dura muy poco y tarda mucho en cargarse.') AS resultado

-- COMMAND ----------

-- MAGIC %md #### ✔️  Analizando todas las reseñas

-- COMMAND ----------

CREATE OR REPLACE TABLE revisiones_revisadas AS
SELECT *, resultado.* FROM (
  SELECT *, revision_evaluacion(avaliacao) as resultado FROM opiniones LIMIT 10)

-- COMMAND ----------

-- MAGIC %md Ahora todos nuestros usuarios pueden aprovechar nuestra función cuidadosamente preparada para analizar las reseñas de nuestros productos.
-- MAGIC
-- MAGIC ¡Y podemos escalar fácilmente este proceso aplicando esta función a todo nuestro conjunto de datos!

-- COMMAND ----------

-- MAGIC %md ## 📝 Generando una respuesta sugerida
-- MAGIC
-- MAGIC Con toda la información extraída, podemos utilizarla para generar sugerencias de respuestas personalizadas para agilizar el trabajo de nuestros equipos de servicio.
-- MAGIC
-- MAGIC Otro punto interesante es que, en este proceso, podemos aprovechar otra **información estructurada** que ya tenemos en nuestro entorno, como datos demográficos, psicográficos e historial de compras, ¡para personalizar aún más nuestras respuestas!
-- MAGIC
-- MAGIC ¡Veamos cómo hacerlo!

-- COMMAND ----------

-- MAGIC %md ### A. Crear una función para generar una respuesta de ejemplo

-- COMMAND ----------

CREATE OR REPLACE FUNCTION GENERAR_RESPUESTA(nombre STRING, apellido STRING, num_pedidos INT, producto STRING, motivo STRING)
RETURNS TABLE(resposta STRING)
COMMENT 'Si el cliente expresa insatisfacción con un producto, utilice esta función para generar una respuesta personalizada'
RETURN SELECT AI_QUERY(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT(
        "Eres un asistente virtual para un comercio electrónico. Nuestro cliente, ", generar_respuesta.nombre, " ", generar_respuesta.apellido, " quien compró ", generar_respuesta.num_pedidos, " productos este año no estaba satisfecho con el producto ", generar_respuesta.producto, 
        ", pues ", generar_respuesta.motivo, ". Proporcionar un breve mensaje empático al cliente incluyendo una oferta para cambiar el producto si cumple con nuestra política de cambio. El canje se podrá realizar directamente a través de este asistente. ",
        "Quiero recuperar su confianza y evitar que deje de ser nuestro cliente. ",
        "Escribe un mensaje con algunas frases. ",
        "No agregue ningún texto que no sea el mensaje. ",
        "No agregues ninguna firma."
    )
)

-- COMMAND ----------

-- MAGIC %md ### B. Generar respuestas automáticas a todas las críticas negativas

-- COMMAND ----------

 CREATE TABLE respuestas AS

WITH avaliacoes_enriq AS (
  SELECT a.*, c.* EXCEPT (c.id_cliente) 
  FROM revisiones_revisadas a 
  LEFT JOIN clientes c 
  ON a.id_cliente = c.id_cliente 
  WHERE a.resposta = 'S' 
  LIMIT 10
)

SELECT 
  *, 
  (SELECT * FROM generar_respuesta(e.nome, e.sobrenome, e.num_pedidos, e.produto_nome, e.resposta_motivo)) AS rascunho 
FROM avaliacoes_enriq e

-- COMMAND ----------

-- MAGIC %md # ¡Felicidades!
-- MAGIC
-- MAGIC ¡Ha completado la práctica de laboratorio de **Extracción de información y generación de texto**!
-- MAGIC
-- MAGIC ¡Ahora ya sabe cómo utilizar funciones de IA para analizar sentimientos e identificar entidades en reseñas de productos de una manera simple y escalable!
