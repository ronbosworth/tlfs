using System.Collections.Generic;
using System;
using System.Text.RegularExpressions;

namespace Tlfs
{
    class LibraryRobot
    {
        private string _changerPath;
        private List<TapeLibrarySlot> _slots;
        private Dictionary<int, TapeLibrarySlot> _driveSlots; //by slot number
        private Dictionary<int, TapeLibrarySlot> _tapeSlots; //by slot number
        public List<TapeLibrarySlot> Slots { get { return _slots; }}
        public Dictionary<int, TapeLibrarySlot> DriveSlots 
        { 
            get
            {
                lock(_updatingStatus)
                {
                    return _driveSlots;
                }
            }
        }
        private readonly object _updatingStatus = new object();
        // public Dictionary<int, TapeLibrarySlot> TapeSlots { get { return _tapeSlots; }}
        public LibraryRobot(string path)
        {
            _changerPath = path;
            UpdateChangerStatus();
        }
        
        public void UpdateChangerStatus()
        {
            lock(_updatingStatus)
            {
                Log.Add(LogLevel.DEBUG, "Parsing changer status.");
                _slots = new List<TapeLibrarySlot>();
                _driveSlots = new Dictionary<int, TapeLibrarySlot>();
                _tapeSlots = new Dictionary<int, TapeLibrarySlot>();
                var elementMatch = new Regex(@"^\s*(Data Transfer Element|Storage Element) (\d{1,})( IMPORT\/EXPORT|):(Full|Empty)(.*:VolumeTag\s?=\s?|)(\w+|)"); //thanks regex101.com
                var output = ProcessHelper.ExecProcess("/usr/sbin/mtx", $"-f {_changerPath} status");
                foreach(var line in output.Split('\n'))
                {
                    var elementMatches = elementMatch.Matches(line);
                    if (elementMatches.Count == 1)
                    {
                        var slot = new TapeLibrarySlot();
                        //[Data Transfer Element] or [Storage Element]
                        switch (elementMatches[0].Groups[1].Value)
                        {
                            case "Data Transfer Element":
                                slot.SlotType = SlotType.DataTransferElement;
                                break;
                            case "Storage Element":
                                slot.SlotType = SlotType.StorageElement;
                                break;
                            default:
                                throw new Exception($"Unknown element type: {elementMatches[0].Groups[1].Value}");
                        }
                        //Slot number
                        int slotNumber;
                        if (int.TryParse(elementMatches[0].Groups[2].Value, out slotNumber))
                        {
                            slot.SlotNumber = slotNumber;
                        }
                        else
                        {
                            throw new Exception($"Could not parse the slot number: {elementMatches[0].Groups[2].Value}");
                        }
                        //[Full] or [Empty]
                        switch (elementMatches[0].Groups[4].Value)
                        {
                            case "Full":
                                slot.Empty = false;
                                break;
                            case "Empty":
                                slot.Empty = true;
                                break;
                            default:
                                throw new Exception($"Slot state was unexpected, not 'Full' or 'Empty': {elementMatches[0].Groups[4].Value}");
                        }
                        if (slot.Empty)
                        {
                            slot.Barcode = null;
                        }
                        else
                        {
                            slot.Barcode = elementMatches[0].Groups[6].Value;
                        }
                        _slots.Add(slot);
                        if (slot.SlotType == SlotType.DataTransferElement)
                        {
                            _driveSlots.Add(slot.SlotNumber, slot);
                        }
                        else
                        {
                            _tapeSlots.Add(slot.SlotNumber, slot);
                        }
                    }
                }
            }
        }

        ///Moves any tapes currenly in a Storage Element to a Data Transfer Element
        public bool MoveAnyTapeToDriveSlot(int driveSlot)
        {
            Log.Add(LogLevel.DEBUG, $"Moving a tape into drive slot {driveSlot}.");
            UpdateChangerStatus();
            foreach(var slot in _slots)
            {
                if (slot.SlotType == SlotType.StorageElement)
                {
                    if (!slot.Empty)
                    {
                        MoveFromSlotToDrive(slot.SlotNumber, driveSlot);
                        UpdateChangerStatus();
                        return true;
                    }
                }
            }
            return false;
        }

        public void MoveTapeToDrive(string barcode, int driveSlot)
        {
            Log.Add(LogLevel.DEBUG, $"Moving tape {barcode} into drive slot {driveSlot}.");
            UpdateChangerStatus();
            foreach(var slot in _tapeSlots.Values)
            {
                if (slot.Barcode == barcode)
                {
                    MoveFromSlotToDrive(slot.SlotNumber, driveSlot);
                    UpdateChangerStatus();
                    return;
                }
            }
            throw new Exception($"Could not find the requested tape {barcode}.");
        }

        ///Uses mtx to move a tape from a Storage Element to a Data Transfer Element 
        private void MoveFromSlotToDrive(int tapeSlot, int driveSlot)
        {
            Log.Add(LogLevel.DEBUG, $"Moving a tape from tape slot {tapeSlot} into drive slot {driveSlot}.");
            ProcessHelper.ExecProcess("/usr/sbin/mtx", $"-f {_changerPath} load {tapeSlot} {driveSlot}");
        }

        ///Moves the tape from the drive slot to any empty tape slot
        public void EjectTape(int driveSlot)
        {
            Log.Add(LogLevel.DEBUG, $"Moving a tape out of drive slot {driveSlot}.");
            UpdateChangerStatus();
            foreach(var slot in _slots)
            {
                if (slot.SlotType == SlotType.StorageElement)
                {
                    if (slot.Empty)
                    {
                        MoveFromDriveToSlot(slot.SlotNumber, driveSlot);
                        UpdateChangerStatus();
                        return;
                    }
                }
            }
        }

        ///Uses mtx to move a tape from a Storage Element to a Data Transfer Element 
        private void MoveFromDriveToSlot(int tapeSlot, int driveSlot)
        {
            Log.Add(LogLevel.DEBUG, $"Moving a tape from drive slot {driveSlot} into tape slot {tapeSlot}.");
            ProcessHelper.ExecProcess("/usr/sbin/mtx", $"-f {_changerPath} unload {tapeSlot} {driveSlot}");
        }

        ///Checks the tape slots for this tape
        ///Todo: make tapelibraryslot comparable so we can use .contains
        public bool IsTapeInDriveSlot(string barcode)
        {
            lock(_updatingStatus)
            {
                foreach(var slot in _driveSlots.Values)
                {
                    if (slot.Barcode == barcode)
                    {
                        return true;
                    }
                }
                return false;
            }
        }
    }
}