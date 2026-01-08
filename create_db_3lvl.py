import os
import itertools

# Определяем базовые символы
SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyz"
ALL_CHARS = SYMBOLS + "symbols"  # "symbols" — отдельная папка/файл как запасной вариант

# Список из 37 "бакетов"
BUCKETS = list(SYMBOLS) + ["symbols"]  # длина = 36 + 1 = 37

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

def create_shard_structure():
    print("📁 Создаю структуру data/...")
    os.makedirs(DATA_DIR, exist_ok=True)
    
    total_files = 0
    for level1 in BUCKETS:
        dir1 = os.path.join(DATA_DIR, level1)
        os.makedirs(dir1, exist_ok=True)
        
        for level2 in BUCKETS:
            dir2 = os.path.join(dir1, level2)
            os.makedirs(dir2, exist_ok=True)
            
            for level3 in BUCKETS:
                # Полный путь к файлу (без расширения!)
                file_path = os.path.join(dir2, level3)
                # Создаём пустой файл
                open(file_path, 'a').close()  # 'a' — создаёт, если не существует
                total_files += 1
                
                # Прогресс (каждые 5000 файлов)
                if total_files % 5000 == 0:
                    print(f"   ✅ Создано {total_files} файлов...")
    
    print(f"\n🎉 Готово! Создано {total_files} пустых файлов в папке 'data'.")
    print(f"   Структура: data/[0-9a-z|symbols]/[0-9a-z|symbols]/[0-9a-z|symbols]")

if __name__ == "__main__":
    create_shard_structure()