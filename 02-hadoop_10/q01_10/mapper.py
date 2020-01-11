#! /usr/bin/python3

#
#  Pregunta
#  ===========================================================================
#
#  El arachivo credit.csv contiene 1000 registros sobre aprobación de creditos. 
#  Cada registro contiene 20 atributos que recopilan información tanto sobre el 
#  crédito como sobre la salud financiera del solicitante. Los atributos son 
#  los siguientes: 
# 
#    Attribute 1:  (qualitative)
#             Status of existing checking account
#             A11 :      ... <    0 DM
#             A12 : 0 <= ... <  200 DM
#             A13 :      ... >= 200 DM /
#                   salary assignments for at least 1 year
#             A14 : no checking account
#  
#    Attribute 2:  (numerical)
#             Duration in month
#  
#    Attribute 3:  (qualitative)
#             Credit history
#             A30 : no credits taken/
#                   all credits paid back duly
#             A31 : all credits at this bank paid back duly
#             A32 : existing credits paid back duly till now
#             A33 : delay in paying off in the past
#             A34 : critical account/
#                   other credits existing (not at this bank)
#  
#    Attribute 4:  (qualitative)
#             Purpose
#             A40 : car (new)
#             A41 : car (used)
#             A42 : furniture/equipment
#             A43 : radio/television
#             A44 : domestic appliances
#             A45 : repairs
#             A46 : education
#             A47 : (vacation - does not exist?)
#             A48 : retraining
#             A49 : business
#             A410 : others
#  
#    Attribute 5:  (numerical)
#             Credit amount
#  
#    Attribute 6:  (qualitative)
#             Savings account/bonds
#             A61 :          ... <  100 DM
#             A62 :   100 <= ... <  500 DM
#             A63 :   500 <= ... < 1000 DM
#             A64 :          .. >= 1000 DM
#             A65 :   unknown/ no savings account
#  
#    Attribute 7:  (qualitative)
#             Present employment since
#             A71 : unemployed
#             A72 :       ... < 1 year
#             A73 : 1  <= ... < 4 years
#             A74 : 4  <= ... < 7 years
#             A75 :       .. >= 7 years
#  
#    Attribute 8:  (numerical)
#             Installment rate in percentage of disposable income
#  
#    Attribute 9:  (qualitative)
#             Personal status and sex
#             A91 : male   : divorced/separated
#             A92 : female : divorced/separated/married
#             A93 : male   : single
#             A94 : male   : married/widowed
#             A95 : female : single
#  
#    Attribute 10: (qualitative)
#             Other debtors / guarantors
#             A101 : none
#             A102 : co-applicant
#             A103 : guarantor
#  
#    Attribute 11: (numerical)
#             Present residence since
#  
#    Attribute 12: (qualitative)
#             Property
#             A121 : real estate
#             A122 : if not A121 : building society savings agreement/
#                    life insurance
#             A123 : if not A121/A122 : car or other, not in attribute 6
#             A124 : unknown / no property
#  
#    Attribute 13: (numerical)
#             Age in years
#  
#    Attribute 14: (qualitative)
#             Other installment plans
#             A141 : bank
#             A142 : stores
#             A143 : none
#  
#    Attribute 15: (qualitative)
#             Housing
#             A151 : rent
#             A152 : own
#             A153 : for free
#  
#    Attribute 16: (numerical)
#             Number of existing credits at this bank
#   
#    Attribute 17: (qualitative)
#             Job
#             A171 : unemployed/ unskilled  - non-resident
#             A172 : unskilled - resident
#             A173 : skilled employee / official
#             A174 : management/ self-employed/
#                    highly qualified employee/ officer
#  
#    Attribute 18: (numerical)
#             Number of people being liable to provide maintenance for
#  
#    Attribute 19: (qualitative)
#             Telephone
#             A191 : none
#             A192 : yes, registered under the customers name
#  
#    Attribute 20: (qualitative)
#             foreign worker
#             A201 : yes
#             A202 : no
#  
#
#  Escriba un job de hadoop (en Python) que compute la cantidad de registros 
#  por cada tipo del atributo `credit_history`.
#

#  El arachivo credit.csv contiene 1000 registros sobre aprobación de creditos. 
#  Cada registro contiene 20 atributos que recopilan información tanto sobre el 
#  crédito como sobre la salud financiera del solicitante.

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
        
        #  Escriba un job de hadoop (en Python) que compute la cantidad de registros 
        #  por cada tipo del atributo `credit_history`.

        word_counter = 0

        ##
        ## imprime un mensaje a la entrada
        ##
        self.status('Iniciando procesamiento ')

        for v in self:
            word_counter += 1
            self.emit(key=v, value=1)
        
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
            yield dat[2]


if __name__ == "__main__":
    ##
    ## inicializa el objeto con el flujo de entrada
    ##
    mapper = Mapper(sys.stdin)

    ##
    ## ejecuta el mapper
    ##
    mapper.map()