function fishping --description "Ping with timestamps"
    ping $argv | while read -l line
        echo (date "+%Y-%m-%d %H:%M:%S") $line
    end
end
