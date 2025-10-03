# Proyecto final

El proyecto de divide en varias partes:

1. Acceso a la API de Red Eléctrica, con el objeto de obtener los datos de demanda eléctrica en los últimos 15 años (código en la carpeta historico_demanda)
   
2. Volcado de la información de la API en un Bucket S3 de AWS, a través de la creación de una primera Lambda (lambda_diaria, en la carpeta lambdas)
   
3. Entrenamiento del modelo de predicción (carpeta modelo), a partir de los datos del histórico de los últimos 15 años de demanda
   
4. Creación de la base de datos de PostgreSQL
   
5. Creación de la segunda lambda (lambda_bbdd en la carpeta lambdas), que envía la información del Bucket S3 a la base de datos de PostgreSQL
    
6. Creación de la API (carpeta FastAPI), que utiliza el modelo creado para resolver consultas sobre demanda futura

7. Creación del EC2 para funcionamiento de la API de FastAPI


#
