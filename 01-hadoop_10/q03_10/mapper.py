#!/usr/bin/env python3
#
#  Pregunta
#  ===========================================================================
#
#  Escriba un job de hadoop (en Python) que ordene el archivo `data.csv` por
#  la segunda columna, de menor a mayor.
#
import sys
#
#  >>> Escriba el codigo del mapper a partir de este punto <<<
#
class Mapper:

    def __init__(self, stream):
        ##
        ## almacena el flujo de entrada como una
        ## variable del objeto
        ##
        self.stream = stream

    def emit(self, key, value):
        ##
        ## escribe al flujo estandar de salida
        ##
        sys.stdout.write("{}\t{}\n".format(key, value))


    def status(self, message):
        ##
        ## imprime un reporte en el flujo de error
        ## no se debe usar el stdout, ya que en este
        ## unicamente deben ir las parejas (key, value)
        ##
        sys.stderr.write('reporter:status:{}\n'.format(message))


    def counter(self, counter, amount=1, group="ApplicationCounter"):
        ##
        ## imprime el valor del contador
        ##
        sys.stderr.write('reporter:counter:{},{},{}\n'.format(group, counter, amount))

    def map(self):

        word_counter = 0

        ##
        ## imprime un mensaje a la entrada
        ##
        self.status('Iniciando procesamiento ')

        for k,v in self:
            ##
            ## cuenta la cantidad de palabras procesadas
            ##
            word_counter += 1

            ##
            ## por cada palabra del flujo de datos
            ## emite la pareja (word, 1)
            ##
            self.emit(key=k, value=v)

        ##
        ## imprime un mensaje a la salida
        ##
        self.counter('num_words', amount=word_counter)
        self.status('Finalizadno procesamiento ')




    def __iter__(self):
        ##
        ## itera sobre cada linea de codigo recibida
        ## a traves del flujo de entrada
        ##
        for line in self.stream:
            ##
            ## itera sobre cada palabra de la linea
            ## (en los ciclos for, retorna las palabras
            ## una a una)
            ##
            dat = line.replace('\n', '').split(',')
            yield dat[1], dat[0]


if __name__ == "__main__":
    ##
    ## inicializa el objeto con el flujo de entrada
    ##
    mapper = Mapper(sys.stdin)

    ##
    ## ejecuta el mapper
    ##
    mapper.map()