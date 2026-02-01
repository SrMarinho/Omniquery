import argparse


def create_parser():
    """Cria e configura o parser de argumentos"""

    parser = argparse.ArgumentParser(
        prog='OmniQuery',
        description='Command line tool for file processing and analysis',
        epilog='Exemplo: python main.py --pipeline pipelines/meu_pipeline.json'
    )

    # Comando: pipeline (arquivo JSON do pipeline)
    parser_pipeline = parser.add_argument(
        '--pipeline',
        type=str,
        required=True,
        help='Path to the pipeline JSON file'
    )

    return parser


def parse_args():
    """Analisa os argumentos da linha de comando"""
    parser = create_parser()

    return parser.parse_args()
