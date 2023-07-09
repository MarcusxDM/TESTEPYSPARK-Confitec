import random
from os import urandom

def generate_rand_matrix():
    """
    Gera numeros randomicos a partir de uma seed especifica do sistema operacional
    popula uma matrix 4x4 e a retorna
    """
    matrix = []
    random.seed(urandom(64))
    for n in range(4):
        line = []
        for i in range(4):
            line.append(random.randrange(1, 10, 1))
        matrix.append(line)
    return matrix
        
def main(a, b):
    """
    Recebe duas matrizes, executa o multiplicacao entre elas
    e as escreve na tela junto com o seu produto
    """
    product = []
    for i in range(len(a)):
        product_line = []
        for j in range(len(b[0])):
            product_sum = 0
            for k in range(len(a)):
                product_sum += (a[i][k] * b[k][j])
            product_line.append(product_sum)
        product.append(product_line)

    print('a =', a)
    print('b =', b)
    print('product =', product)

if __name__ == '__main__':
    a = generate_rand_matrix()
    b = generate_rand_matrix()
    main(a, b)