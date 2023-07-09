from algoritmo import generate_rand_matrix, main

def test_generate_rand_matrix():
    """
    Testa se a matriz gerada randomicamente possui tamanho 4x4
    e se seus numeros sao inteiros entre 1 e 10
    """
    matrix = generate_rand_matrix()
    assert len(matrix) == 4
    assert len(matrix[0]) == 4

    for line in matrix:
        for number in line:
            assert isinstance(number, int)
            assert 1 <= number <= 10

def test_main(capsys):
    """
    Utiliza duas matrizes exemplo para testar a funcao de multiplicacao de matrizes e
    verifica se o resultado no output eh o esperado
    """
    a = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 1, 2], [3, 4, 5, 6]]
    b = [[7, 8, 9, 10], [1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 1, 2]]

    # Execucao da funcao de multiplicacao
    main(a, b)
    result = capsys.readouterr()

    expected_output = "a = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 1, 2], [3, 4, 5, 6]]\n" \
                      "b = [[7, 8, 9, 10], [1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 1, 2]]\n" \
                      "product = [[60, 70, 40, 50], [148, 174, 120, 146], [96, 118, 120, 142], [104, 122, 80, 98]]\n"
    assert result.out == expected_output