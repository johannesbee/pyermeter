#!/usr/bin/python

# Python code to read values from Smart Meter via SML (smart message language)

import sys
import serial
import time
import datetime

#mqtt
import paho.mqtt.client as mqtt
mqttClient = mqtt.Client()

#mqtt config
mqttClient.connect("mqttb-ct", 1883, 60)
mqttPrefix="u7/powermeters/test/"


## Variablen:
## 62,63,65,69 = Unsigned Int 8/16/32/64
## 52,53,55,59 = Integer 8/16/32/64


def hex_to_int(hex):
    #mask for two complements detection
    mask = 0x8<<((len(hex)-1)*4)

    i = int(hex, 16)
    if i & mask:    # MSB set -> neg.

        # xor oparand for twos complement, according to digits of number
        xor = '0xf'
        for x in range(1, len(hex)):
            xor = xor +'f'
        xor = int(xor,16)

        #print 'negativ i: '+str(i) + ' xor: '+str(xor)
        return -((~i & xor) + 1)
    else:
        return i

scalerPrefix ='52' #number values seems to begin with it
scalerSuffix1 = '00' #0 Nachkommastellen
scalerSuffix10 = 'ff' # 1 Nachkommastellen
scalerSuffix20 = 'fe' # 2 Nachkommastellen
scaler = {scalerSuffix1, scalerSuffix10, scalerSuffix20}

def DecodeObis(identifier, dataset):
    retval = 0.0
    posStartObis = dataset.find(identifier)
    scale = 1
    if (posStartObis <> -1):
        #find begin of value
        pos = posStartObis + len(identifier)
        posEndObis = dataset.find('017707', posStartObis)

        for suf in scaler:
            #print 'Search: '+ scalerPrefix+suf
            pos = dataset.find(scalerPrefix+suf, posStartObis)
            #print 'Search: '+ scalerPrefix+suf + ' Pos: '+str(pos)
            if pos == -1:
                continue
            #print 'pos: '+ str(pos)
            #print 'posEndValue: '+ str(posEndObis)
            if suf == '00':
                scale = 1
            elif suf == 'ff':
                scale = 10
            elif suf == 'fe':
                scale = 100

            if (pos < posEndObis):
                break

        posStartValue = pos + 6


        value = dataset[posStartValue:posEndObis]
        print 'Debug: '+ dataset[posStartValue-4:posEndObis]
        retVal = float( hex_to_int(value)) / scale
        return retVal


port = serial.Serial(
    port='/dev/ttyUSB0',
    baudrate=9600,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS
)

#port.open();

start = '1b1b1b1b01010101'
end = '1b1b1b1b1a'



data = ''

now = datetime.datetime.now()
starttimestamp = (now.strftime("%Y-%m-%d ") + now.strftime("%H:%M:%S"))
prevminute = 321


while True:
    char = port.read()
    data = data + char.encode('HEX')
    pos = data.find(end)
    if (pos <> -1):

        now = datetime.datetime.now()
        print str(now)

        energy = 0
        search = '77070100010800'
        try:
            energy = float(DecodeObis(search, data))
            print 'Total Bezug (1.8.0): ' + str(energy/1e3) + ' kWh'
        except:
            pass

        search = '77070100020800'
        try:
            energy_feed = float(DecodeObis(search, data))
            print 'Total Lieferung (2.8.0): ' + str(energy_feed/1e3) + ' kWh'
        except:
            pass


        search = '77070100100700ff01'
        try:
            power = float(DecodeObis(search, data))
            print 'Leistung (16.7.0): ' + str(power) + ' W'
            mqttClient.publish(mqttPrefix+"power16.7.0", str(power).strip())
        except:
            pass


        search = '77070100200700ff'
        try:
            UL1 = DecodeObis(search, data)
            print 'U L1 (32.7.0): ' + str(UL1) + ' V'
        except:
            pass


        search = '77070100340700ff'
        try:
            UL2 = DecodeObis(search, data)
            print 'U L2 (52.7.0): ' + str(UL2) + ' V'
        except:
            pass


        search = '77070100480700ff'
        try:
            UL3 = float(DecodeObis(search, data))
            print 'U L3 (72.7.0): ' + str(UL3) + ' V'
        except:
            pass


        search = '770701001f0700ff'
        try:
            IL1 = float(DecodeObis(search, data))
            print 'I L1 (31.7.0): ' + str(IL1) + ' A'
        except:
            pass


        search = '77070100330700ff'
        try:
            IL2 = float(DecodeObis(search, data))
            print 'I L2 (51.7.0): ' + str(IL2) + ' A'
        except:
            pass


        search = '77070100470700ff'
        try:
            IL3 = float(DecodeObis(search, data))
            print 'I L3 (71.7.0): ' + str(IL3) + ' A'
        except:
            pass


        search = '77070100510701ff'
        try:
            pa_UL2_UL1= float(DecodeObis(search, data))
            print 'Phasenwinkel U-L2 zu U-L1 (81.7.1): ' + str(pa_UL2_UL1) + ' Deg'
        except:
            pass


        search = '77070100510702ff'
        try:
            pa_UL3_UL1= float(DecodeObis(search, data))
            print 'Phasenwinkel U-L2 zu U-L1 (81.7.2): ' + str(pa_UL3_UL1) + ' Deg'
        except:
            pass


        search = '77070100510704ff'
        try:
            pa_IL3_UL1= float(DecodeObis(search, data))
            print 'Phasenwinkel U-L2 zu U-L1 (81.7.4): ' + str(pa_IL3_UL1) + ' Deg'
        except:
            pass


        search = '7707010051070fff'
        try:
            pa_IL2_UL2= float(DecodeObis(search, data))
            print 'Phasenwinkel U-L2 zu U-L1 (81.7.15): ' + str(pa_IL2_UL2) + ' Deg'
        except:
            pass


        search = '7707010051071aff'
        try:
            pa_IL3_UL3= float(DecodeObis(search, data))
            print 'Phasenwinkel U-L2 zu U-L1 (81.7.26): ' + str(pa_IL3_UL3) + ' Deg'
        except:
            pass


        search = '770701000e0700ff'
        try:
            Freq = float(DecodeObis(search, data))
            print 'Frequenz (14.7.0): ' + str(Freq) + ' Hz'
        except:
            pass

        print ' '
        data = ''
