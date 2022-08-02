# sparkLinealRegression

Clases de Scala que utilizan Spark para calcular Regresiones lineales de dos variables, a partir de archivos CSV. 
Además, existe una clase para verificar el modelo, calculando RMSE, MAE y MAPE. 

## Entradas

### cenfotec.ambientesdistribuidos.LinealRegression

Para la clase "cenfotec.ambientesdistribuidos.LinealRegression" son necesarios 2 archivos, los cuales se especifican como parametro en el siguiente orden: 
1. Un archivo CSV que recoge las observaciones. El orden de las columnas debe ser:
  1. X1, la primer variable estudiada
  2. X2, la sgunda variable estudiada
  3. Y: El valor observado, basado en X1 y X2
2. Un archivo de salida, donde se escribirá el modelo resultante. El formato de la salida es:
  1. M: EL valor donde el modelo interseca los ejes X1 y X2
  2. A: El coeficiente por el que se debe multiplicar X1
  3. B: El coeficiente por el que se debe multiplicar X2

_NOTA_: Ningún archivo CSV debe de contener encabezados

### cenfotec.ambientesdistribuidos.ModelEvaluation
Para esta clase, se requieren dos entradas, las cuales se especifican como parametro en el siguiente orden:
1. El modelo a evaluar, el formato es igual a la salida de la clase anterior
2. EL conjunto de datos de prueba. El formato es idéntico al del prmer parametro de la clase anterior.

# Como ejecutar el programa:
## Usando linea de comandos
El proyecto está compilado con el plugin de Scala, por lo que no es necesario intalar el lenguaje para su compilación y ejecucion.

Para ejecutar el programa en un ambiente local, se pueden ejecutar los siguientes comandos: 

```shell
#compilar la clase
mvn clean package

#ejecutar el generador del modelo
java -Dspark.master=local -cp target/proyecto-final-1.0-SNAPSHOT-jar-with-dependencies.jar cenfotec.ambientesdistribuidos.LinealRegression
file:///path/to/file/model-input.csv file:///path/to/file/out.csv

#ejecutar el evaluador
java -Dspark.master=local -cp target/proyecto-final-1.0-SNAPSHOT-jar-with-dependencies.jar cenfotec.ambientesdistribuidos.ModelEvaluation file:///path/to/file/out.csv file:///path/to/file/regression-test.csv
```

## Usando Spark Submit
También es posible enviar el jar a un cluster de Spark para su ejecución: 
```shell
#compilar la clase
mvn clean package

#ejecutar el generador del modelo
spark-submit --class cenfotec.ambientesdistribuidos.LinealRegression target/proyecto-final-1.0-SNAPSHOT.jar file:///path/to/file/model-input.csv file:///path/to/file/out.csv

#ejecutar el evaluador
spark-submit --class cenfotec.ambientesdistribuidos.ModelEvaluation  target/proyecto-final-1.0-SNAPSHOT.jar file:///path/to/file/out.csv file:///path/to/file/regression-test.csv
```