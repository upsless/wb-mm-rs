pub fn dbus_connected_message() -> &'static str {
    "DBus connection established"
}

pub fn dbus_stopped_before_connect_message() -> &'static str {
    "DBus loop stopped before connection was established"
}

pub fn dbus_stopped_message() -> &'static str {
    "DBus connection closed"
}
