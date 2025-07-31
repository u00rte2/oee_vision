def validate_user():
	valid_users = [ "TEnglund", "TGeissler", "Zach.Merrilees"]
	username = system.tag.readBlocking(["[System]Client/User/Username"])[0].value
	if username in valid_users:
		return True
	return False


def open_admin_window(window_path):
	if validate_user():
		system.nav.openWindow(window_path)
		system.nav.centerWindow(window_path)
	return