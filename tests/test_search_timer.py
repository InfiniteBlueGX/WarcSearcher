from search_timer import SearchTimer

class TestSearchTimer:
    def test1(self):
        searchTimer = SearchTimer()
        assert searchTimer.start_time is None

    def test2(self):
        searchTimer = SearchTimer()
        searchTimer.start_timer()
        assert searchTimer.start_time is not None