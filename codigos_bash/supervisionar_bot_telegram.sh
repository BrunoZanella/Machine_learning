
# #!/bin/bash
# # supervisionar_bot_telegram.sh

# # Caminho para o seu script Python
# SCRIPT_PYTHON="/home/bruno/codigos/bot.py"

# # Configure o PYTHONPATH
# export PYTHONPATH="/home/bruno/codigos"

# # Variável de controle para rastrear se a mensagem já foi impressa
# mensagem_impressa=0

# # Verifica se o script Python está em execução
# while true; do
#     if pgrep -f $SCRIPT_PYTHON > /dev/null; then
#         # Se a mensagem ainda não foi impressa, imprima-a
#         if [[ "$mensagem_impressa" -eq 0 ]]; then
#             echo "$(date '+%Y-%m-%d %H:%M:%S') - O script $SCRIPT_PYTHON já está em execução."
#             # Atualize a variável de controle
#             mensagem_impressa=1
#         fi
#     else
#         # Se o script não está em execução, reinicie-o e redefina a variável de controle
#         echo "$(date '+%Y-%m-%d %H:%M:%S') - O script $SCRIPT_PYTHON não está em execução. Iniciando..."
#         until python3 $SCRIPT_PYTHON; do
#             echo "$(date '+%Y-%m-%d %H:%M:%S') - O script $SCRIPT_PYTHON foi encerrado com um erro. Reiniciando..." >&2
#             sleep 1
#         done
#         mensagem_impressa=0
#     fi
#     # Aguarde um pouco antes de verificar novamente
#     sleep 1
# done


#!/bin/bash
# supervisionar_bot_telegram.sh

# Caminho para o seu script Python
SCRIPT_PYTHON="/home/bruno/codigos/bot.py"

# Configure o PYTHONPATH
export PYTHONPATH="/home/bruno/codigos"

# Variável de controle para rastrear se a mensagem já foi impressa
mensagem_impressa=0

# Verifica se o script Python está em execução
while true; do
    if pgrep -f $SCRIPT_PYTHON > /dev/null; then
        # Se a mensagem ainda não foi impressa, imprima-a
        if [[ "$mensagem_impressa" -eq 0 ]]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - O script $SCRIPT_PYTHON já está em execução."
            # Atualize a variável de controle
            mensagem_impressa=1
        fi
    else
        # Se o script não está em execução, reinicie-o e redefina a variável de controle
        echo "$(date '+%Y-%m-%d %H:%M:%S') - O script $SCRIPT_PYTHON não está em execução. Iniciando..."
        until python3 $SCRIPT_PYTHON; do
            echo "$(date '+%Y-%m-%d %H:%M:%S') - O script $SCRIPT_PYTHON foi encerrado com um erro. Reiniciando..." >&2
            sleep 1
        done
        mensagem_impressa=0
    fi
    # Aguarde um pouco antes de verificar novamente
    sleep 1
done


