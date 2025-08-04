import os
import csv
import threading
import concurrent.futures
from collections import defaultdict
from tqdm import tqdm
import multiprocessing

lock = threading.Lock()

# 线程安全的存储
folder_sizes = defaultdict(int)
file_sizes = {}

def get_folder_and_files_size(path, folder_min_size=0, file_min_size=0):
    folder_total = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path, topdown=True, onerror=lambda e: None):
            # 统计文件夹内所有文件大小
            for f in filenames:
                try:
                    fp = os.path.join(dirpath, f)
                    if os.path.exists(fp):
                        size = os.path.getsize(fp)
                        if size >= file_min_size:
                            with lock:
                                file_sizes[fp] = size
                        folder_total += size
                except:
                    pass
        # 文件夹大小达到阈值才存储
        if folder_total >= folder_min_size:
            with lock:
                folder_sizes[path] = folder_total
    except Exception:
        pass

def find_all_directories(start_path):
    all_dirs = []
    for dirpath, dirnames, _ in os.walk(start_path, topdown=True, onerror=lambda e: None):
        for d in dirnames:
            full_path = os.path.join(dirpath, d)
            all_dirs.append(full_path)
    return all_dirs

def get_sensible_thread_count():
    cpu_count = multiprocessing.cpu_count()
    return min(cpu_count * 10, 64)

def unique_filename(base_name):
    # 找到不重复的文件名，类似 文件夹结果1.csv 文件夹结果2.csv ...
    count = 1
    while True:
        filename = f"{base_name}{count}.csv"
        if not os.path.exists(filename):
            return filename
        count += 1

def scan_all(drive_letter, folder_min_mb=0, file_min_mb=0, top_n=50):
    drive_path = f"{drive_letter}:\\"
    folder_min_size = folder_min_mb * 1024 * 1024
    file_min_size = file_min_mb * 1024 * 1024

    print(f"扫描盘符：{drive_path}")
    print(f"文件夹大小阈值（MB）：{folder_min_mb}")
    print(f"文件大小阈值（MB）：{file_min_mb}")

    max_workers = get_sensible_thread_count()
    print(f"推荐线程数：{max_workers}，开始收集目录...")

    all_dirs = find_all_directories(drive_path)
    print(f"共发现 {len(all_dirs)} 个文件夹，开始扫描大小和文件...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        progress = tqdm(total=len(all_dirs), desc="扫描中", unit="文件夹")

        for path in all_dirs:
            future = executor.submit(get_folder_and_files_size, path, folder_min_size, file_min_size)
            future.add_done_callback(lambda p: progress.update())
            futures.append(future)

        concurrent.futures.wait(futures)
        progress.close()

    # 排序并取前N
    sorted_folders = sorted(folder_sizes.items(), key=lambda x: x[1], reverse=True)[:top_n]
    sorted_files = sorted(file_sizes.items(), key=lambda x: x[1], reverse=True)[:top_n]

    # 生成唯一文件名
    folder_csv = unique_filename("文件夹结果")
    file_csv = unique_filename("文件结果")

    # 写入文件夹大小结果CSV
    with open(folder_csv, "w", newline='', encoding='utf-8-sig') as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["文件夹路径", "大小（字节）", "大小（MB）"])
        for path, size in sorted_folders:
            writer.writerow([path, size, round(size / (1024*1024), 2)])

    # 写入文件大小结果CSV
    with open(file_csv, "w", newline='', encoding='utf-8-sig') as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["文件路径", "大小（字节）", "大小（MB）"])
        for path, size in sorted_files:
            writer.writerow([path, size, round(size / (1024*1024), 2)])

    print(f"\n扫描完成，已导出文件夹大小到 {folder_csv}")
    print(f"扫描完成，已导出文件大小到 {file_csv}")

if __name__ == "__main__":
    drive_letter = input("请输入盘符字母（如 C）：").strip().upper()
    folder_min_mb = input("请输入文件夹大小保留阈值（MB，默认0，不过滤）：").strip()
    file_min_mb = input("请输入文件大小保留阈值（MB，默认0，不过滤）：").strip()

    folder_min_mb = int(folder_min_mb) if folder_min_mb.isdigit() else 0
    file_min_mb = int(file_min_mb) if file_min_mb.isdigit() else 0

    print("建议以管理员权限运行此程序以避免权限问题。")
    scan_all(drive_letter, folder_min_mb, file_min_mb, top_n=50)
