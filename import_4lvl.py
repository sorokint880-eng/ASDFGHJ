import os
import sys
import glob
import time
from collections import defaultdict

# Настройки
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
IMPORT_DIR = os.path.join(SCRIPT_DIR, "import")
BUFFER_SIZE_PER_SHARD = 500  # Сколько строк буферизировать перед записью на диск
BUCKETS = list("0123456789abcdefghijklmnopqrstuvwxyz") + ["symbols"]  # 37 бакетов

# Создаём папку import, если её нет
os.makedirs(IMPORT_DIR, exist_ok=True)
print(f"📁 Папка для импорта: {IMPORT_DIR}")
print(f"   (положи туда файлы в подпапки, если нужно)")

def get_shard_path(email: str) -> str:
    """Определяет путь к файлу шарда по первым 4 символам email"""
    email_prefix = email[:4].lower()
    
    # Если email короче 4 символов — дополняем 'symbols'
    while len(email_prefix) < 4:
        email_prefix += 'symbols'[len(email_prefix) % 7]  # Чередуем символы из "symbols"
    
    levels = []
    for char in email_prefix[:4]:  # Берём ровно 4 символа
        if char in "0123456789abcdefghijklmnopqrstuvwxyz":
            levels.append(char)
        else:
            levels.append("symbols")
    
    return os.path.join(DATA_DIR, levels[0], levels[1], levels[2], levels[3])

def normalize_email(email: str) -> str:
    """Нормализует email: только нижний регистр, удаляем пробелы"""
    return email.strip().lower()

def load_existing_pairs(shard_path: str) -> set:
    """Загружает существующие пары из файла шарда как множество строк 'email:id'"""
    if not os.path.exists(shard_path):
        return set()
    
    try:
        with open(shard_path, 'r', encoding='utf-8', errors='ignore') as f:
            return {line.strip() for line in f if line.strip()}
    except Exception as e:
        print(f"⚠️ Ошибка чтения шарда {shard_path}: {e}")
        return set()

def main():
    # Шаг 1: спрашиваем параметры у пользователя
    separator = input("Введите разделитель в файлах (например : или ;): ").strip()
    if not separator:
        separator = ":"
        print(f"   Использую разделитель по умолчанию: '{separator}'")
    
    ext = input("Введите расширение файлов для импорта (например .txt или .csv). Нажмите Enter для всех файлов: ").strip()
    if ext:
        if not ext.startswith('.'):
            ext = f".{ext}"
        print(f"   Буду искать файлы с расширением: '{ext}'")
    else:
        print("   Буду обрабатывать ВСЕ файлы в папке import/")
    
    # Шаг 2: собираем все файлы для импорта
    if ext:
        search_pattern = os.path.join(IMPORT_DIR, f"**/*{ext}")
    else:
        search_pattern = os.path.join(IMPORT_DIR, "**/*")
    
    import_files = glob.glob(search_pattern, recursive=True)
    
    # Фильтруем служебные файлы (скрытые, временные, системные)
    ignore_patterns = {".git", ".svn", ".DS_Store", "Thumbs.db", ".tmp", "~"}
    import_files = [
        f for f in import_files
        if not any(p in f for p in ignore_patterns) and os.path.isfile(f)
    ]
    
    if not import_files:
        print(f"\n❌ Не найдено подходящих файлов в папке '{IMPORT_DIR}'.")
        print("   Положите файлы в эту папку и запустите скрипт снова.")
        return
    
    print(f"\n🔍 Найдено {len(import_files)} файлов для обработки. Начинаю импорт...")
    start_time = time.time()
    total_processed = 0
    total_added = 0
    
    # Кэш для шардов: путь -> множество существующих пар
    shard_cache = {}
    # Буфер для записи: путь -> список новых строк
    write_buffer = defaultdict(list)
    
    try:
        for file_idx, file_path in enumerate(import_files, 1):
            filename = os.path.relpath(file_path, IMPORT_DIR)
            print(f"\n📚 [{file_idx}/{len(import_files)}] Обрабатываю: {filename}")
            file_start = time.time()
            file_added = 0
            
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Разделяем по указанному разделителю (только первое вхождение)
                        if separator not in line:
                            continue
                        
                        email_part, id_part = line.split(separator, 1)
                        email = normalize_email(email_part)
                        id_val = id_part.strip()
                        
                        if not email or not id_val:
                            continue
                        
                        # Формируем нормализованную пару
                        pair_str = f"{email}:{id_val}"
                        
                        # Определяем шард (4 уровня!)
                        shard_path = get_shard_path(email)
                        
                        # Загружаем кэш шарда, если ещё не загружен
                        if shard_path not in shard_cache:
                            shard_cache[shard_path] = load_existing_pairs(shard_path)
                        
                        # Проверяем уникальность пары
                        if pair_str not in shard_cache[shard_path]:
                            # Добавляем в буфер
                            write_buffer[shard_path].append(pair_str)
                            shard_cache[shard_path].add(pair_str)
                            file_added += 1
                            total_added += 1
                            
                            # Если буфер переполнен — сбрасываем на диск
                            if len(write_buffer[shard_path]) >= BUFFER_SIZE_PER_SHARD:
                                flush_buffer(shard_path, write_buffer[shard_path])
                                write_buffer[shard_path] = []
                        
                        total_processed += 1
                        
                        # Статус каждые 100к строк
                        if total_processed % 100_000 == 0:
                            elapsed = time.time() - start_time
                            speed = total_processed / elapsed if elapsed > 0 else 0
                            print(f"   📈 Прогресс: {total_processed:,} строк | "
                                  f"Добавлено: {total_added:,} | "
                                  f"Скорость: {speed:.0f} строк/сек", end='\r')
            
            except Exception as e:
                print(f"\n⚠️ Ошибка при чтении {filename}: {e}")
            
            print(f"✅ Файл обработан за {time.time() - file_start:.1f} сек. "
                  f"Добавлено: {file_added:,} уникальных пар")
        
        # Шаг 3: сбрасываем оставшиеся данные из буферов
        print("\n💾 Сохраняю оставшиеся данные...")
        for shard_path, buffer in write_buffer.items():
            if buffer:
                flush_buffer(shard_path, buffer)
        
        # Финальная статистика
        elapsed = time.time() - start_time
        print(f"\n🎉 ИМПОРТ ЗАВЕРШЕН!")
        print(f"✅ Всего обработано строк: {total_processed:,}")
        print(f"✅ Уникальных пар добавлено: {total_added:,}")
        print(f"⏱️ Общее время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        if elapsed > 0:
            print(f"🚀 Средняя скорость: {total_processed/elapsed:.0f} строк/сек")
    
    except KeyboardInterrupt:
        print("\n\n🛑 Импорт прерван пользователем. Сохраняю текущие данные...")
        for shard_path, buffer in write_buffer.items():
            if buffer:
                flush_buffer(shard_path, buffer)
        print("✅ Промежуточные данные сохранены. Продолжи позже.")
        sys.exit(1)

def flush_buffer(shard_path: str, buffer: list):
    """Записывает буфер в файл шарда, создавая папки при необходимости"""
    try:
        # Создаём папки, если их нет
        os.makedirs(os.path.dirname(shard_path), exist_ok=True)
        
        # Добавляем в конец файла (append)
        with open(shard_path, 'a', encoding='utf-8') as f:
            f.write("\n".join(buffer) + ("\n" if buffer else ""))
    except Exception as e:
        print(f"❌ Ошибка записи в шард {shard_path}: {e}")

if __name__ == "__main__":
    # Проверяем, существует ли структура data/
    if not os.path.exists(DATA_DIR):
        print(f"\n❌ Папка 'data' не найдена!")
        print("   Сначала запусти скрипт создания 4-уровневой структуры.")
        sys.exit(1)
    
    main()