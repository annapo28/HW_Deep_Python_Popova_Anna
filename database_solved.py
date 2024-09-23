from __future__ import annotations
import hashlib
import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Protocol, Optional, Dict, Tuple

class SupportsStr(Protocol):
    """Класс с абстрактным методом `__str__`."""
    def __str__(self) -> str:
        """Привести тип к `str`."""

@dataclass
class StorageAdapter:
    """Адаптер базы данных."""
    _storage_directory: Path
    _pending_operations: Dict[str, Tuple[str, Optional[str]]] = field(default_factory=dict)

    def _get_file_path(self, key: SupportsStr) -> Path:
        """Возвращает путь к файлу для данного ключа."""
        key_hash = hashlib.sha256(str(key).encode()).hexdigest()
        return self._storage_directory / key_hash

    def _compute_value_hash(self, key: str, value: str) -> str:
        """Вычисляет хеш значения."""
        hasher = hashlib.sha256()
        hasher.update(key.encode())
        hasher.update(value.encode())
        return hasher.hexdigest()

    def _load_from_file(self, file_path: Path) -> Optional[Dict[str, str]]:
        """Загружает и проверяет данные из файла. Возвращает словарь данных или None."""
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                expected_hash = self._compute_value_hash(data['key'], data['value'])
                if data['hash'] == expected_hash:
                    return data
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            pass
        return None

    def get(self, key: SupportsStr) -> Optional[str]:
        """Получить объект, если он существует. Возвращает значение или None."""
        if str(key) in self._pending_operations:
            op_type, _ = self._pending_operations[str(key)]
            if op_type == 'delete':
                previous_data = self._load_from_file(self._get_file_path(key))
                return previous_data['value'] if previous_data else None

        file_path = self._get_file_path(key)
        data = self._load_from_file(file_path)
        return data['value'] if data else None

    def update(self, key: SupportsStr, value: SupportsStr) -> None:
        """Обновить (или добавить) значение по ключу."""
        self._pending_operations[str(key)] = ('update', str(value))

    def delete(self, key: SupportsStr) -> None:
        """Удалить ключ вместе со значением."""
        self._pending_operations[str(key)] = ('delete', None)

    def clear(self) -> None:
        """Удалить все ключи вместе со значениями."""
        self._pending_operations.clear()
        self._pending_operations['__clear__'] = ('clear', None)

    def commit(self) -> None:
        """Подтвердить изменения."""
        if '__clear__' in self._pending_operations:
            shutil.rmtree(self._storage_directory, ignore_errors=True)
            self._storage_directory.mkdir(parents=True, exist_ok=True)
            self._pending_operations.pop('__clear__')

        if self._storage_directory.exists():
            if self._storage_directory.is_file():
                self._storage_directory.unlink()
            elif self._storage_directory.is_symlink():
                self._storage_directory.unlink()

        if not self._storage_directory.exists():
            self._storage_directory.mkdir(parents=True, exist_ok=True)

        for key, (op_type, value) in self._pending_operations.items():
            file_path = self._get_file_path(key)

            try:
                if op_type == 'update':
                    data = {
                        'key': str(key),
                        'value': str(value),
                        'hash': self._compute_value_hash(str(key), str(value))
                    }
                    file_path.write_text(json.dumps(data))
                elif op_type == 'delete' and file_path.exists():
                    file_path.unlink()
            except (FileNotFoundError, OSError) as e:
                print(f"Error during commit operation: {e}")
                raise

        self._pending_operations.clear()

    def rollback(self) -> None:
        """Откатить неподтвержденные изменения."""
        self._pending_operations.clear()

    def __getitem__(self, key: SupportsStr) -> Optional[str]:
        """Получить объект, если он существует. Возвращает значение или None."""
        return self.get(key)

    def __setitem__(self, key: SupportsStr, value: SupportsStr) -> None:
        """Обновить (или добавить) значение по ключу."""
        self.update(key, value)

    def __delitem__(self, key: SupportsStr) -> None:
        """Удалить ключ вместе со значением."""
        self.delete(key)

    def __enter__(self) -> StorageAdapter:
        """Открыть транзакцию. Проверка директории. Из-за того, что я не делала это отдельно при каждом энтере, получалось проблема с симлинкой и записью в одно место, несмотря на то, что я использовала with. Отедельная проверка : """
        if self._storage_directory.is_symlink() or self._storage_directory.is_file():
            try:
                self._storage_directory.unlink()
            except Exception:
                raise RuntimeError(f"Failed to remove existing path {self._storage_directory}: {Exception}")

        try:
            self._storage_directory.mkdir(parents=True, exist_ok=True)
        except FileExistsError:  
            if not self._storage_directory.is_dir():
                raise RuntimeError(f"Path {self._storage_directory} exists but is not a directory.")
        return self

    def __exit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc_value: Optional[BaseException],
            traceback: Optional[TracebackType],
    ) -> None:
        """Закрыть транзакцию."""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
