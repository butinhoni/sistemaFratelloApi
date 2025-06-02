import logging
from logging.handlers import RotatingFileHandler

def configurar_logging(nome_arquivo='log.log', nivel=logging.DEBUG):
    # Evita adicionar múltiplos handlers se já estiverem configurados
    if logging.getLogger().hasHandlers():
        return

    # Cria o handler com rotação
    file_handler = RotatingFileHandler(
        nome_arquivo,
        maxBytes=1024 * 100,  # 100 KB
        backupCount=5,
        encoding='utf-8'
    )

    # Formato do log
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d/%m/%Y %H:%M:%S'
    )
    file_handler.setFormatter(formatter)

    # Stream handler (console)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Configura o logger
    logging.basicConfig(
        level=nivel,
        handlers=[file_handler, console_handler]
    )
