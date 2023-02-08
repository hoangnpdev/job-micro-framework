package nph.utils;

import java.util.Arrays;

public class BankUtils {
    private static String[] banks  = {
            "ABANK",
            "ABBANK",
            "ACB",
            "AGRIBANK",
            "AGRIBANK-BR",
            "AGRIBANK - KT",
            "AGRIBANK10",
            "AGRIBANKCN9",
            "AGRIBANKLTK",
            "Agribank HN",
            "AgribankPDP",
            "Agribank_BD",
            "Agribank_LD",
            "BACABANK",
            "BANK2GO.VN",
            "BANKTCKCB",
            "BAOVIETBank",
            "BIDV",
            "BangkokBank",
            "CBBank",
            "CIMB BANK",
            "CITIBANK",
            "Co-opBank",
            "DBS Bank",
            "DongA Bank",
            "Eximbank",
            "GPBANK",
            "HDBank",
            "HSBC",
            "LPB",
            "MBBANK",
            "MSB",
            "NH.KienLong",
            "NHBanViet",
            "NHCSXH",
            "NamABank",
            "OCB",
            "OCEANBANK",
            "PGBank",
            "PVcomBank",
            "PublicBank",
            "RAPBANK",
            "SCB",
            "SHB",
            "Sacombank",
            "SaiGonBank",
            "SeABank",
            "ShinhanBank",
            "TARGOBANK",
            "TPBank",
            "TVAM",
            "Techcombank",
            "VIB",
            "VIETBANK",
            "VIETCOMBANK",
            "VPBANK",
            "VPBank",
            "VietABank",
            "Vietcombank",
            "VietinBank",
            "WOORIVN",
            "ubank.vn",
            "NHQUOCDAN",
            "StanChart",
            "IVB",
            "VRB",
            "BHXHANGIANG",
            "BHXHQ12",
            "BHXH.QPN",
            "BHXH.Q4",
            "BHXH.QGV",
            "BHXHNQ_HP",
            "BHXH_NTMY",
            "BHXH.TPTD",
            "BHXHVLONG",
            "BHXH.HHM",
            "BHXHCAMPHA",
            "VPBANK_KYBA",
            "BankSinopac",
            "BANKER.VN",
            "HDFCBank",
            "BACABANK-LC",
            "JTRBank",
            "BPI Bank",
            "Agribank.ST",
            "BANVIETBANK",
            "CITIBANKHK",
            "BanVietBank",
    };

    public static boolean contain(String bank) {
        return Arrays.asList(banks).contains(bank);
    }
}
