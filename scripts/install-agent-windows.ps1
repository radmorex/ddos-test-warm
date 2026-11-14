# Установка агента LoadSwarm на Windows как службы через Планировщик задач.
# Запускать в PowerShell ОТ ИМЕНИ АДМИНИСТРАТОРА из папки проекта.
# Требования: Python с python.org (при установке отметить "Add to PATH").

$ErrorActionPreference = "Stop"

$dir = "C:\loadswarm"
New-Item -ItemType Directory -Force $dir | Out-Null
Copy-Item "$PSScriptRoot\..\agent.py" $dir -Force
python -m pip install --quiet aiohttp

$controller = Read-Host "URL контроллера (например http://1.2.3.4:8080)"
$token      = Read-Host "токен"
$name       = $env:COMPUTERNAME

# Обёртка .cmd: чтобы планировщик запускал одной строкой и писал лог
@"
@echo off
python $dir\agent.py --controller $controller --token $token --name $name >> $dir\agent.log 2>&1
"@ | Set-Content "$dir\run.cmd" -Encoding ASCII

schtasks /Create /TN "LoadSwarmAgent" /SC ONSTART /RU SYSTEM /TR "$dir\run.cmd" /F

Write-Host "`nГотово. Задача 'LoadSwarmAgent' стартует при загрузке системы."
Write-Host "Ручной запуск сейчас:  schtasks /Run /TN LoadSwarmAgent"
Write-Host "Лог: $dir\agent.log"
