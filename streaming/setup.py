"""Empacota validacao.py para o worker do Dataflow.

O pipeline.py vira o arquivo principal e e enviado sozinho; o contrato.py e
o montagem.py, que ele importa, nao vao junto. Sem este setup.py o job sobe
e morre com ModuleNotFoundError na primeira mensagem.

Usado pelo --setup_file do Beam, nao para instalar nada a mao. As
dependencias ficam em requirements.txt.
"""

from setuptools import setup

setup(
    name="f1-streaming",
    version="1.0.0",
    description="Montagem da volta a partir do dado bruto da OpenF1",
    py_modules=["contrato", "montagem", "predicao"],
)
