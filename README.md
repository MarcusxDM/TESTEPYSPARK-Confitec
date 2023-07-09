# TESTEPYSPARK-Confitec
 Teste proficiente para a vaga de Engenheiro de Dados

# Configuração das Credenciais da AWS

Para enviar o arquivo CSV para o Amazon S3, é preciso configurar corretamente as credenciais da AWS.

## 1. Configuração das Credenciais da AWS

É necessário ter as credenciais da AWS para realizar operações com serviços como o Amazon S3.
Anote a **Chave de acesso** (Access Key) e a **Chave de acesso secreta** (Secret Access Key) fornecidas.

## 2. Arquivo `credentials.py`

Após obter as credenciais da AWS, você deve criar um arquivo chamado `credentials.py` e adicionar as informações das chaves de acesso:

```python
# credentials.py

access_key_id = "sua_access_key_id"
secret_access_key = "sua_secret_access_key"
region_name = "sua_região_da_aws"
```
## 3. Arquivo `requirements.txt`

Para executar este código, você precisará instalar algumas bibliotecas Python. Execute o pip install com o arquivo `requirements.txt`:

```
pip install -r requirements.txt
```
