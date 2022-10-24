import pandas as pd
import os as listar

exitenciaArchivoParquet = False

##parquet_file= r'part-00000-67492399-6512-44cd-acb3-afd0f1ebee6b-c000.zstd.parquet'

contenidos = listar.listdir('../result.parquet')

def mensaje():
    print("---------------------------------------FINISH APLICATION----------------------------------------------")

for contenido in contenidos:
    nombre, extension = listar.path.splitext(contenido)
    if extension == ".parquet":
        print("------------------------------------STARTING APLICATION--------------------------------------------------")
        print("--------------------LEYENDO DATOS PARQUET MODO FASTPARQUET-----------------------------------------------")
        print(pd.read_parquet(contenido, engine='fastparquet'))
        print("---------------------------------------------------------------------------------------------------------")
        print("FINISH FASTPARQUET")
        print("---------------------------------------------------------------------------------------------------------")
        print("--------------------------------LEYENDO DATOS PARQUET MODO PYARROW---------------------------------------")
        print(pd.read_parquet(contenido, engine='pyarrow'))
        print("---------------------------------------------------------------------------------------------------------")
        print("FINISH FASTPARQUET")
        print("---------------------------------------------------------------------------------------------------------")
        print("START HEAD")
        dataF = pd.read_parquet(contenido, engine='fastparquet')
        print(dataF.head())
        print("FINISH HEAD")
        exitenciaArchivoParquet = True
    

if exitenciaArchivoParquet :
    mensaje()
else :
    print("----------------------------------------NO HAY ARCHIVOS CON FORMATO PARQUET-------------------------------")
    mensaje()









