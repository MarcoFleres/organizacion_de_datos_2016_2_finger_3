# coding=utf-8
from random import randint

def jonesPlassman(rows, nodes, edges):
    ''' El algoritmo consiste en:
    * Asignar un número aleatorio a cada elemento.
    * Por cada uno, si es el mayor de sus vecinos que no tienen color, se le asigna uno.
    '''

    nodeNeighbors = rows\
        .map(lambda a: (a[0], [a[1]])).reduceByKey(lambda a, b: a+b)\
        .map(lambda a: (a[0], ((-1), a[1])))

    nodeWeights = {randint(0, nodes) for i in xrange(0, nodeNeighbors.count())}

    # (nodo, (color, [vecinos]))

    print(nodeNeighbors.filter(lambda a: a[1][1] == (-1)).count())



    #print(nodeNeighbors.takeOrdered(5, lambda a : -a[1][0]))







def execute(sc):

    # Jones Plassmann
    # Largest-Degree-First
    # Smallest-Degree_Last <-----

    input = sc.textFile('/home/marco/Documentos/Organización de Datos/finger/3/repo/gc_1000_1')
    header = input.take(1)[0]
    lineas = int(header.split()[0])
    aristas = int(header.split()[1])

    rows = input\
        .filter(lambda line: line != header)\
        .map(lambda line: line.split())\
        .map(lambda a: (int(a[0]), int(a[1])))

    jonesPlassman(rows, lineas, aristas)

    # Algoritmo Smallest Degree Last

    # k = 1
    # p = 1
    # Mientras haya nodos sin peso:
    #   Buscar todos los nodos de grado <= k, y asignarles peso p
    #   p++
    #   k++
    # Ordenar los nodos por peso, y asignaeles un colo distinto al de todos sus vecinos, en orden.
    #