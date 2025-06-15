#define FUSE_USE_VERSION 31

#include <fuse3/fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <libgen.h>

// -------------------- STRUCTURES --------------------
typedef struct dir_entry {
    char name[255];
    struct inode* node;
    struct dir_entry* next;
} dir_entry;

typedef struct inode {
    int inode_id;
    int file_type; // 1 = file, 2 = dir
    char access_rights[10];
    char name[255];
    int file_size;
    char* create_time;
    int start_block;
    int total_blocks;
    char* content;
    
    // Для иерархии файловой системы
    struct dir_entry* children;
    char* initial_content; // Текст для записи при создании
    struct inode* parent;  // Родительский inode

    // Для глобального списка inode
    struct inode* next_inode;
} inode;

typedef struct superblock {
    int total_inodes; 
    int total_blocks;
    int free_blocks;
    int free_inodes;
    int block_size;
    inode* inodes_table;
    char* time_mount;
    inode* root;
    int next_inode_id; // Счетчик для назначения ID
} superblock;

superblock sb;

// -------------------- HELPERS --------------------
void shift_inode_ids(int deleted_id) { // Сдвигает идентификаторы inode после удаления, поддерживая целостность системы
    inode* curr = sb.inodes_table;
    while (curr) {
        if (curr->inode_id > deleted_id) {
            curr->inode_id--;
        }
        curr = curr->next_inode;
    }

    // Обновим dir_entry структуры, если нужно
    curr = sb.inodes_table;
    while (curr) {
        dir_entry* entry = curr->children;
        while (entry) {
            if (entry->node->inode_id > deleted_id) {
                entry->node->inode_id--;
            }
            entry = entry->next;
        }
        curr = curr->next_inode;
    }

    // Обновим next_inode_id в суперблоке
    if (sb.next_inode_id > 0) {
        sb.next_inode_id--;
    }
}




char* current_time_str() { // Генерирует текущее время в строковом формате
    time_t t = time(NULL);
    char* buf = malloc(26);
    strftime(buf, 26, "%Y-%m-%d %H:%M:%S", localtime(&t));
    return buf;
}

inode* create_inode(int type, const char* rights, const char* name, const char* init_content, inode* parent) { // Создает новый inode с заданными параметрами и связью с родителем
    inode* node = malloc(sizeof(inode));
    node->inode_id = sb.next_inode_id++;
    node->file_type = type;
    strncpy(node->access_rights, rights, sizeof(node->access_rights));
    strncpy(node->name, name, sizeof(node->name));
    node->file_size = 0;
    node->create_time = current_time_str();
    node->start_block = -1; // Пока не выделен
    node->total_blocks = 0;
    node->content = NULL;
    node->children = NULL;
    node->initial_content = init_content ? strdup(init_content) : NULL;
    node->next_inode = NULL;
    node->parent = parent;
    return node;
}

void add_entry_to_dir(inode* dir, inode* child) { // Добавляет дочерний элемент в директорию через список записей
    dir_entry* new_entry = malloc(sizeof(dir_entry));
    strncpy(new_entry->name, child->name, sizeof(new_entry->name));
    new_entry->node = child;
    new_entry->next = dir->children;
    dir->children = new_entry;
}

inode* find_inode_in_dir(inode* dir, const char* name) { // Ищет inode по имени в дочерних элементах указанной директории
    if (!dir) return NULL;
    dir_entry* entry = dir->children;
    while (entry) {
        if (strcmp(entry->name, name) == 0) {
            return entry->node;
        }
        entry = entry->next;
    }
    return NULL;
}

inode* find_inode_by_path(const char* path) { // Находит inode по абсолютному пути в файловой системе
    if (strcmp(path, "/") == 0) return sb.root;
    
    // Пропускаем начальный слэш
    const char* p = path[0] == '/' ? path + 1 : path;
    inode* current = sb.root;
    
    char* path_copy = strdup(p);
    char* comp = strtok(path_copy, "/");
    
    while (comp && current) {
        current = find_inode_in_dir(current, comp);
        comp = strtok(NULL, "/");
    }
    
    free(path_copy);
    return current;
}

// Рекурсивная функция для построения дерева
void build_tree_string(inode* node, char* buffer, int* pos, int max_size, int depth) { // Рекурсивно формирует строковое представление дерева файловой системы
    if (*pos >= max_size - 200) return; // Оставляем место
    
    // Добавляем отступы в зависимости от глубины
    char indent[128] = {0};
    for (int i = 0; i < depth; i++) {
        strcat(indent, "│   ");
    }
    
    // Формируем информацию о текущем узле
    char node_info[256];
    snprintf(node_info, sizeof(node_info), "%s├── %s (%s, inode: %d, size: %d)\n", 
             indent, node->name, 
             node->file_type == 1 ? "file" : "dir",
             node->inode_id, node->file_size);
    
    // Добавляем в буфер
    int len = strlen(node_info);
    if (*pos + len < max_size) {
        strcpy(buffer + *pos, node_info);
        *pos += len;
    }
    
    // Если это директория, обрабатываем детей
    if (node->file_type == 2) {
        dir_entry* child = node->children;
        while (child) {
            build_tree_string(child->node, buffer, pos, max_size, depth + 1);
            child = child->next;
        }
    }
}

// -------------------- FUSE OPERATIONS --------------------
static int myfs_getattr(const char* path, struct stat* stbuf, struct fuse_file_info* fi) { // stat (получение метаданных) // Возвращает метаданные файла/директории (реализует команду stat)
    memset(stbuf, 0, sizeof(struct stat));

    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    if (strcmp(path, "/superblock") == 0) {
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = 1024;
        return 0;
    }

    inode* node = find_inode_by_path(path);
    if (node) {
        stbuf->st_mode = (node->file_type == 1 ? S_IFREG : S_IFDIR) | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = node->file_size;
        return 0;
    }

    return -ENOENT;
}

static int myfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,  
                        off_t offset, struct fuse_file_info* fi,
                        enum fuse_readdir_flags flags) { // ls (чтение директории) // Читает содержимое директории (реализует команду ls)
    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);
    filler(buf, "superblock", NULL, 0, 0);

    inode* dir = (strcmp(path, "/") == 0) ? sb.root : find_inode_by_path(path);
    if (!dir || dir->file_type != 2) return -ENOENT;

    dir_entry* entry = dir->children;
    while (entry) {
        filler(buf, entry->name, NULL, 0, 0);
        entry = entry->next;
    }
    return 0;
}

static int myfs_create(const char* path, mode_t mode, struct fuse_file_info* fi) { // touch (создание файла с контентом) // Создает новый файл с возможностью инициализации контентом из имени
    // Создаем копии для безопасной обработки
    char* path_copy1 = strdup(path);
    char* path_copy2 = strdup(path);
    
    char* dir_path = dirname(path_copy1);
    char* base = basename(path_copy2);
    
    // Извлекаем текст после '=' в имени файла
    char* file_name = base;
    char* init_content = NULL;
    char* eq_pos = strchr(base, '=');
    if (eq_pos) {
        *eq_pos = '\0'; // Отделяем имя файла
        init_content = eq_pos + 1;
    }

    inode* parent;
    if (strcmp(dir_path, "/") == 0) {
        parent = sb.root;
    } else {
        parent = find_inode_by_path(dir_path);
    }
    
    if (!parent || parent->file_type != 2) {
        free(path_copy1);
        free(path_copy2);
        return -ENOENT;
    }

    if (find_inode_in_dir(parent, file_name)) {
        free(path_copy1);
        free(path_copy2);
        return -EEXIST;
    }

    inode* node = create_inode(1, "rw-r--r--", file_name, init_content, parent);
    
    // Выделяем блок для файла
    if (sb.free_blocks > 0) {
        node->start_block = sb.total_blocks - sb.free_blocks;
        node->total_blocks = 1;
        sb.free_blocks--;
    } else {
        free(path_copy1);
        free(path_copy2);
        return -ENOSPC; // Нет свободных блоков
    }
    
    node->content = calloc(1, sb.block_size);
    
    // Если есть начальный контент, копируем его
    if (init_content) {
    size_t len = strlen(init_content);
    if (len >= sb.block_size) len = sb.block_size - 1;

    memcpy(node->content, init_content, len);
    
    // Добавляем \n, если его нет
    if (init_content[len - 1] != '\n') {
        node->content[len] = '\n';
        node->file_size = len + 1;
    } else {
        node->file_size = len;
    }
}
    
    // Добавляем в родительскую директорию
    add_entry_to_dir(parent, node);
    
    // Добавляем в глобальный список inode
    node->next_inode = sb.inodes_table;
    sb.inodes_table = node;
    sb.total_inodes++;
    sb.free_inodes--;
    
    free(path_copy1);
    free(path_copy2);
    return 0;
}

static int myfs_mkdir(const char* path, mode_t mode) { // mkdir (создание директории) // Создает новую директорию
    // Создаем копии для безопасной обработки
    char* path_copy1 = strdup(path);
    char* path_copy2 = strdup(path);
    
    char* dir_path = dirname(path_copy1);
    char* dir_name = basename(path_copy2);
    
    inode* parent;
    if (strcmp(dir_path, "/") == 0) {
        parent = sb.root;
    } else {
        parent = find_inode_by_path(dir_path);
    }
    if (!parent || parent->file_type != 2) {
        free(path_copy1);
        free(path_copy2);
        return -ENOENT;
    }
    
    if (find_inode_in_dir(parent, dir_name)) {
        free(path_copy1);
        free(path_copy2);
        return -EEXIST;
    }

    inode* node = create_inode(2, "rwxr-xr-x", dir_name, NULL, parent);
    
    // Добавляем в родительскую директорию
    add_entry_to_dir(parent, node);
    
    // Добавляем в глобальный список inode
    node->next_inode = sb.inodes_table;
    sb.inodes_table = node;
    sb.total_inodes++;
    sb.free_inodes--;
    
    free(path_copy1);
    free(path_copy2);
    return 0;
}

static void free_dir_entries(dir_entry* entry) { // Рекурсивно освобождает память записей каталога
    while (entry) {
        dir_entry* next = entry->next;
        free(entry);
        entry = next;
    }
}

static int remove_entry_from_dir(inode* parent, const char* name) {
    if (!parent) return -1;
    dir_entry** entry_ptr = &parent->children;
    while (*entry_ptr) {
        if (strcmp((*entry_ptr)->name, name) == 0) {
            dir_entry* to_delete = *entry_ptr;
            *entry_ptr = to_delete->next;
            free(to_delete);
            return 0;
        }
        entry_ptr = &(*entry_ptr)->next;
    }
    return -1;
}

static int myfs_rmdir(const char* path) { // rmdir (удаление директории) // Удаляет пустую директорию
    inode* node = find_inode_by_path(path);
    if (!node || node->file_type != 2) 
        return -ENOTDIR;
    if (node->children) 
        return -ENOTEMPTY;

    // Создаем копии для безопасной обработки
    char* path_copy1 = strdup(path);
    char* path_copy2 = strdup(path);
    
    char* dir_path = dirname(path_copy1);
    char* dir_name = basename(path_copy2);
    
    inode* parent = node->parent;
    if (!parent) {
        free(path_copy1);
        free(path_copy2);
        return -ENOENT;
    }

    remove_entry_from_dir(parent, dir_name);
    
    // Удаляем из глобального списка inode
    inode** curr = &sb.inodes_table;
    while (*curr) {
        if (*curr == node) {
            *curr = node->next_inode;
            free(node->create_time);
            free_dir_entries(node->children);
            if (node->initial_content) free(node->initial_content);
            if (node->content) free(node->content);
            int deleted_id = node->inode_id;
            free(node);
            sb.free_inodes++;
            shift_inode_ids(deleted_id);
            free(path_copy1);
            free(path_copy2);
            return 0;
        }
        curr = &(*curr)->next_inode;
    }
    
    free(path_copy1);
    free(path_copy2);
    return -ENOENT;
}

static int myfs_unlink(const char* path) { // rm (удаление файла) // Удаляет файл (реализует команду rm)
    inode* node = find_inode_by_path(path);
    if (!node || node->file_type != 1) 
        return -ENOENT;

    // Создаем копии для безопасной обработки
    char* path_copy1 = strdup(path);
    char* path_copy2 = strdup(path);
    
    char* dir_path = dirname(path_copy1);
    char* file_name = basename(path_copy2);
    
    inode* parent = node->parent;
    if (!parent) {
        free(path_copy1);
        free(path_copy2);
        return -ENOENT;
    }

    remove_entry_from_dir(parent, file_name);
    
    // Освобождаем блоки
    sb.free_blocks += node->total_blocks;
    node->total_blocks = 0;
    
    // Удаляем из глобального списка inode
    inode** curr = &sb.inodes_table;
    while (*curr) {
        if (*curr == node) {
            *curr = node->next_inode;
            free(node->create_time);
            if (node->initial_content) free(node->initial_content);
            if (node->content) free(node->content);
            int deleted_id = node->inode_id;
            free(node);
            sb.free_inodes++;
            shift_inode_ids(deleted_id);
            free(path_copy1);
            free(path_copy2);
            return 0;
        }
        curr = &(*curr)->next_inode;
    }
    
    free(path_copy1);
    free(path_copy2);
    return -ENOENT;
}

static int myfs_read(const char* path, char* buf, size_t size, off_t offset,
                     struct fuse_file_info* fi) { // cat (чтение файлов + системной информации) // Читает содержимое файла или системной информации
    if (strcmp(path, "/superblock") == 0) {
        char output[16384]; // Увеличим буфер для дерева
        int pos = snprintf(output, sizeof(output), "Superblock Info:\n"
                 "Total inodes: %d\n"
                 "Free inodes: %d\n"
                 "Total blocks: %d\n"
                 "Free blocks: %d\n"
                 "Block size: %d\n"
                 "Mounted at: %s\n\n",
                 sb.total_inodes, sb.free_inodes, sb.total_blocks,
                 sb.free_blocks, sb.block_size, sb.time_mount);
        
        // Добавляем дерево файловой системы
        pos += snprintf(output + pos, sizeof(output) - pos, "File system tree:\n");
        build_tree_string(sb.root, output, &pos, sizeof(output), 0);
        
        // Добавляем список inodes
        inode* curr = sb.inodes_table;
        while (curr && pos < (int)sizeof(output) - 200) {
            char info[512];
            int n = snprintf(info, sizeof(info),
                    "Inode %d: %s, type=%s, size=%d, blocks=%d, rights=%s, created=%s\n",
                    curr->inode_id, curr->name,
                    curr->file_type == 1 ? "file" : "dir",
                    curr->file_size, curr->total_blocks,
                    curr->access_rights, curr->create_time);
            if (pos + n < (int)sizeof(output)) {
                strcpy(output + pos, info);
                pos += n;
            } else {
                break;
            }
            curr = curr->next_inode;
        }

        size_t len = strlen(output);
        if (offset < len) {
            if (offset + size > len) size = len - offset;
            memcpy(buf, output + offset, size);
        } else {
            size = 0;
        }
        return size;
    }

    inode* node = find_inode_by_path(path);
    if (node && node->file_type == 1 && node->content) {
        if (offset < node->file_size) {
            if (offset + size > node->file_size) 
                size = node->file_size - offset;
            memcpy(buf, node->content + offset, size);
        } else {
            size = 0;
        }
        return size;
    }

    return -ENOENT;
}

static int myfs_write(const char* path, const char* buf, size_t size,
                      off_t offset, struct fuse_file_info* fi) { // echo > (запись в файл) // Записывает данные в файл (с ограничением размера блока)
    inode* node = find_inode_by_path(path);
    if (!node || node->file_type != 1) return -ENOENT;

    if (!node->content) {
        // Выделяем блок, если еще не выделен
        if (sb.free_blocks <= 0) return -ENOSPC;
        node->content = calloc(1, sb.block_size);
        node->total_blocks = 1;
        node->start_block = sb.total_blocks - sb.free_blocks;
        sb.free_blocks--;
    }
    
    if (offset + size > sb.block_size) {
        // Не поддерживаем запись за пределы одного блока
        size = sb.block_size - offset;
    }
    
    if (size > 0) {
        memcpy(node->content + offset, buf, size);
        if (offset + size > node->file_size) {
            node->file_size = offset + size;
        }
    }
    
    return size;
}

static int myfs_open(const char* path, struct fuse_file_info* fi) { // проверка доступа // Проверяет существование файла перед открытием
    if (strcmp(path, "/superblock") == 0) return 0;
    return (find_inode_by_path(path) != NULL) ? 0 : -ENOENT;
}

static int fs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) { // Заглушка для обновления временных меток (не реализована)
    return 0;  // просто ничего не делаем
}

// -------------------- MAIN --------------------
static struct fuse_operations myfs_oper = {
    .getattr = myfs_getattr,    // stat (получение метаданных)
    .readdir = myfs_readdir,    // ls (чтение директории)
    .create  = myfs_create,     // touch (создание файла с контентом)
    .mkdir   = myfs_mkdir,      // mkdir (создание директории)
    .rmdir   = myfs_rmdir,      // rmdir (удаление директории)
    .unlink  = myfs_unlink,     // rm (удаление файла)
    .read    = myfs_read,       // cat (чтение файлов + системной информации)
    .write   = myfs_write,      // echo > (запись в файл)
    .open    = myfs_open,       // проверка доступа
    .utimens = fs_utimens,    
};

int main(int argc, char* argv[]) { // Инициализирует суперблок и корневую директорию, запускает FUSE
    sb.total_inodes = 0; 
    sb.total_blocks = 1024;
    sb.free_blocks = 1024;
    sb.free_inodes = 100;
    sb.block_size = 4096;
    sb.inodes_table = NULL;
    sb.time_mount = current_time_str();
    sb.next_inode_id = 1; // Начинаем с 1
    
    // Создаем корневую директорию
    sb.root = create_inode(2, "rwxr-xr-x", "/", NULL, NULL);
    sb.root->inode_id = 0; // Особый ID для корня
    // Добавляем корень в глобальный список
    sb.root->next_inode = sb.inodes_table;
    sb.inodes_table = sb.root;
    sb.total_inodes = 1;
    sb.free_inodes--;

    return fuse_main(argc, argv, &myfs_oper, NULL);
}