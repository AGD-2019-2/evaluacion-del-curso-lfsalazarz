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
#  >>> Escriba el codigo del reducer a partir de este punto <<<
#
class Reducer:

    def __init__(self, stream):
        self.stream = stream

    def emit(self, key, value):
        sys.stdout.write("{},{}\n".format(key, value))

    def reduce(self):
        ##
        ## Esta funcion reduce los elementos que
        ## tienen la misma clave
        ##
        for k, v in self:
            self.emit(key=k, value=v)

    def __iter__(self):

        for line in self.stream:
            ##
            ## Lee el stream de datos y lo parte
            ## en (clave, valor)
            ##
            
            key, val = line.replace('\n', '').split("\t")
            
            ##
            ## retorna la tupla (clave, valor)
            ## como el siguiente elemento del ciclo for
            ##
            yield val, key


if __name__ == '__main__':

    reducer = Reducer(sys.stdin)
    reducer.reduce()