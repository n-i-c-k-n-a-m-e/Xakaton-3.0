import pandas as pd
import glob

class UserSession:
    def __init__(self, id_session, id_subscriber, start_session, end_session, up_tx, down_tx):
        self.id_session = id_session
        self.id_subscriber = id_subscriber
        self.start_session = pd.to_datetime(start_session)  # Преобразуем в datetime
        self.end_session = pd.to_datetime(end_session)      # Преобразуем в datetime
        self.up_tx = up_tx                                  # Количество отданных данных в МБ
        self.down_tx = down_tx                              # Количество загруженных данных в МБ

    def session_duration(self):
        return (self.end_session - self.start_session).total_seconds()  # Возвращает продолжительность сессии в секундах

    def is_compromised(self):
        # Установим пороговые значения
        time_threshold = 24 * 3600  # 24 часа в секундах
        data_tx_threshold = 10 * 1024  # 10 ГБ в МБ

        # Логика определения зараженности
        if (self.session_duration() > time_threshold and
            (self.up_tx > data_tx_threshold or self.down_tx > data_tx_threshold)):
            return True
        return False

    def detect_traffic_spike(self, sessions):
        # Определяем средний объем трафика для данного пользователя
        user_sessions = [s for s in sessions if s.id_subscriber == self.id_subscriber]
        total_up_tx = sum(session.up_tx for session in user_sessions)
        total_down_tx = sum(session.down_tx for session in user_sessions)
        average_up_tx = total_up_tx / len(user_sessions) if user_sessions else 0
        average_down_tx = total_down_tx / len(user_sessions) if user_sessions else 0

        # Определяем порог для резкого увеличения трафика
        spike_threshold = 2  # Увеличение в 2 раза от среднего

        if (self.up_tx > average_up_tx * spike_threshold or
            self.down_tx > average_down_tx * spike_threshold):
            return True
        return False

def main():
    # Загрузка данных из всех CSV файлов в текущем каталоге
    all_files = glob.glob("*.csv")  # Замените на путь к вашим файлам, если необходимо
    df_list = []

    for filename in all_files:
        df = pd.read_csv(filename)
        df_list.append(df)

    # Объединяем все DataFrame в один
    all_data = pd.concat(df_list, ignore_index=True)

    # Загрузка контактных данных из файла Parquet
    contacts_df = pd.read_parquet('contacts.parquet')  # Замените на путь к вашему файлу Parquet

    # Создаем список сессий
    sessions = [UserSession(
        id_session=row['IdSession'],
        id_subscriber=row['IdSubscriber'],
        start_session=row['StartSession'],
        end_session=row['EndSession'],
        up_tx=row['UpTx'],
        down_tx=row['DownTx']
    ) for index, row in all_data.iterrows()]

    # Создаем витрины с интервалом в 1 час
    hourly_summary = []

    # Проверяем каждую сессию
    for session in sessions:
        compromised = session.is_compromised()
        traffic_spike = session.detect_traffic_spike(sessions)

        # Определяем статус пользователя
        status = 'clean'
        justification = ''
        
        if compromised:
            status = 'compromised'
            justification = 'Session duration exceeded threshold and data transfer is high.'
        elif traffic_spike:
            status = 'suspicious'
            justification = 'Traffic spike detected compared to average usage.'

        # Получаем контактные данные пользователя
        contact_info = contacts_df[contacts_df['IdSubscriber'] == session.id_subscriber]
        if not contact_info.empty:
            contact = contact_info.iloc[0]  # Предполагаем, что контактные данные уникальны
            hourly_summary.append({
                'username': contact['Username'],
                'status': status,
                'justification': justification,
                'contact': contact['Contact']
            })

    # Преобразуем в DataFrame и сохраняем в файл
    summary_df = pd.DataFrame(hourly_summary)
    summary_df.to_csv('hourly_summary.csv', index=False)

    # Вывод результатов
    print(summary_df)

if __name__ == "__main__":
    main()