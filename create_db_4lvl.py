import os
import time
import sys

# Настройки
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
BUCKETS = list("0123456789abcdefghijklmnopqrstuvwxyz") + ["symbols"]  # 37 бакетов
TOTAL_FILES = len(BUCKETS) ** 4  # 37^4 = 1_874_161

def create_shard_structure_4_levels():
    print(f"🔥 ВНИМАНИЕ! Создание {TOTAL_FILES:,} файлов займёт:")
    print(f"   - ~7.2 ГБ дискового пространства (пустые файлы на NTFS)")
    print(f"   - 30-90 минут на SSD, 2-6 часов на HDD")
    confirm = input("\n❓ Продолжить? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Отменено пользователем.")
        sys.exit(0)
    
    print(f"\n📁 Создаю 4-уровневую структуру в {DATA_DIR}...")
    os.makedirs(DATA_DIR, exist_ok=True)
    
    start_time = time.time()
    total_created = 0
    last_update = time.time()
    
    try:
        for l1 in BUCKETS:
            dir1 = os.path.join(DATA_DIR, l1)
            os.makedirs(dir1, exist_ok=True)
            
            for l2 in BUCKETS:
                dir2 = os.path.join(dir1, l2)
                os.makedirs(dir2, exist_ok=True)
                
                for l3 in BUCKETS:
                    dir3 = os.path.join(dir2, l3)
                    os.makedirs(dir3, exist_ok=True)
                    
                    for l4 in BUCKETS:
                        file_path = os.path.join(dir3, l4)  # Файл без расширения
                        try:
                            # Создаём пустой файл с минимальными накладными расходами
                            with open(file_path, 'a'):
                                os.utime(file_path, None)  # Обновляем timestamp
                        except Exception as e:
                            print(f"\n⚠️ Ошибка создания {file_path}: {e}")
                        
                        total_created += 1
                        
                        # Обновляем прогресс каждые 5 секунд или 50к файлов
                        current_time = time.time()
                        if current_time - last_update > 5 or total_created % 50_000 == 0:
                            elapsed = current_time - start_time
                            speed = total_created / elapsed if elapsed > 0 else 0
                            percent = (total_created / TOTAL_FILES) * 100
                            print(f"   📊 {total_created:,} / {TOTAL_FILES:,} файлов "
                                  f"({percent:.1f}%) | "
                                  f"Скорость: {speed:.0f} файлов/сек", end='\r')
                            last_update = current_time
    
    except KeyboardInterrupt:
        print("\n\n🛑 Создание прервано пользователем. Частично создана структура.")
        sys.exit(1)
    
    elapsed = time.time() - start_time
    print(f"\n\n🎉 СТРУКТУРА СОЗДАНА!")
    print(f"✅ Всего файлов: {total_created:,} (ожидалось {TOTAL_FILES:,})")
    print(f"⏱️ Время выполнения: {elapsed/60:.1f} минут")
    print(f"🔍 Проверка первого файла: {os.path.join(DATA_DIR, '0/0/0/0')}")

if __name__ == "__main__":
    # Быстрая проверка свободного места (минимум 10 ГБ)
    try:
        import shutil
        _, _, free = shutil.disk_usage(SCRIPT_DIR)
        if free < 10 * 1024**3:  # 10 ГБ
            print(f"⚠️ ВНИМАНИЕ: На диске осталось мало места! Свободно: {free/(1024**3):.1f} ГБ")
            print("   Для создания структуры требуется минимум 10 ГБ свободного места.")
            confirm = input("Продолжить? (y/n): ").strip().lower()
            if confirm != 'y':
                sys.exit(0)
    except:
        pass
    
    create_shard_structure_4_levels()