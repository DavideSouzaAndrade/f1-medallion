"""Empacota validacao.py para o worker do Dataflow.

O pipeline.py vira o arquivo principal e e enviado sozinho; o validacao.py,
que ele importa, nao vai junto. Sem este setup.py o job sobe e morre com
ModuleNotFoundError no primeiro evento.

Usado pelo --setup_file do Beam, nao para instalar nada a mao. As
dependencias ficam em requirements.txt.
"""

from setuptools import setup

setup(
    name="f1-streaming",
    version="1.0.0",
    description="Validacao do evento de volta no pipeline Dataflow",
    py_modules=["validacao"],
)
