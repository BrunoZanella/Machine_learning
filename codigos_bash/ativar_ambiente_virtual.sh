#!/bin/bash
# Ativar o ambiente virtual

echo "Ativando o ambiente virtual..."
source /home/bruno/codigos/venv/bin/activate > /dev/null 2>&1
if [[ "$?" -ne 0 ]]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Falha ao ativar o ambiente virtual."
    exit 1
fi
echo "$(date '+%Y-%m-%d %H:%M:%S') - Ambiente virtual ativado com sucesso."

# Variáveis de controle para rastrear se a mensagem já foi impressa
declare -A script_to_log=( ["supervisionar_bot_telegram.sh"]="log_bot.txt" )
declare -A script_printed=( ["supervisionar_bot_telegram.sh"]=0 )

# Verificar e executar os scripts
for script in "${!script_to_log[@]}"; do
    if ! pgrep -f $script > /dev/null; then
        echo "Executando $script..."
        nohup /home/bruno/codigos/codigos_bash/$script > /home/bruno/logs/${script_to_log[$script]} 2>&1 &
        script_printed[$script]=0
    else
        # Se a mensagem ainda não foi impressa, imprima-a
        if [[ "${script_printed[$script]}" -eq 0 ]]; then
            echo "O script $script já está em execução."
            script_printed[$script]=1
        fi
    fi
done
