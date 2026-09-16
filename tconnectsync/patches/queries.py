# Replacement definitions applied to the pinned upstream module at image build.
def time_range(field_name, start_time, end_time, t_to_space=False):
    # Retain the signature, but never generate the legacy space-separated format.
    params = {}
    if start_time is not None:
        params[f'find[{field_name}][$gte]'] = format_datetime(start_time)
    if end_time is not None:
        params[f'find[{field_name}][$lte]'] = format_datetime(end_time)
    return '&' + urllib.parse.urlencode(params) if params else ''


class QueryFix:
    def _last_uploaded(self, endpoint, filters, date_field, time_start, time_end):
        params = {'count': 1, **filters, 'ts': str(time.time())}
        query = urllib.parse.urlencode(params) + time_range(date_field, time_start, time_end)
        response = requests.get(urljoin(self.url, endpoint) + '?' + query, headers={
            'api-secret': hashlib.sha1(self.secret.encode()).hexdigest()
        }, verify=self.verify, timeout=30)
        # A failed read must not masquerade as an empty history and permit duplicates.
        if response.status_code != 200:
            raise ApiException(response.status_code, 'Nightscout history lookup failed')
        records = response.json()
        if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
            raise ValueError('Nightscout history response must be a list of records')
        return records[0] if records else None

    def last_uploaded_entry(self, eventType, time_start=None, time_end=None):
        return self._last_uploaded('api/v1/treatments', {
            'find[enteredBy]': ENTERED_BY, 'find[eventType]': eventType
        }, 'created_at', time_start, time_end)

    def last_uploaded_bg_entry(self, time_start=None, time_end=None):
        return self._last_uploaded('api/v1/entries.json', {
            'find[device]': ENTERED_BY
        }, 'dateString', time_start, time_end)

    def last_uploaded_activity(self, activityType, time_start=None, time_end=None):
        return self._last_uploaded('api/v1/activity', {
            'find[enteredBy]': ENTERED_BY, 'find[activityType]': activityType
        }, 'created_at', time_start, time_end)

    def last_uploaded_devicestatus(self, time_start=None, time_end=None):
        return self._last_uploaded('api/v1/devicestatus', {
            'find[device]': ENTERED_BY
        }, 'created_at', time_start, time_end)
