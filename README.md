# rahw

1. Клонируем себе репу, запускаем командой
```bash
docker compose up -d
```
После запуска у вас должна быть доступна постгря по порту 6543 и должен открываться интерфейс Airflow по localhost:8080

Посмотреть список контейнеров:
```bash
docker ps 
```
Почитать логи:
```bash
docker logs <container_id> 
```

2. Регистрируемся на https://api.nasa.gov/ , заполучаем ключ
3. Берем любой метод в котором в запросе можно прокинуть дату, например "Asteroids - NeoWs". Анализируем возращаемые данные. Определяемся с полями, в сулчае с "Asteroids - NeoWs" предлагаю взять следующие поля:
   * id
   * neo_reference_id
   * name
   * nasa_jpl_url
   * absolute_magnitude_h
   * estimated_diameter.meters.estimated_diameter_min
   * estimated_diameter.meters.estimated_diameter_max
   * load_date - брать из "верхнего уровня" сообщения
4. Пишем DDL под эти данные, создаем таблицу
5. Пилим даг со следующими требованиями:
   * даг должен запускаться первого числа каждого месяца в 03:00 по Мск, начиная с 2022-01-01
   * 
