const lambda = require('../handler');
const sinon = require("sinon");
const expect = require("chai").expect;
const HashMap = require("hashmap");

const WEIGHTS = {
    "twn": [1.2, 1, 1.5],
    "tha": [1.2, 1, 1.5],
    "idn": [1.2, 1, 1.5],
    "kor": [1.2, 1, 2]
};

const genres = {
    Items: [
        {genre_code: 'DRAMA'},
        {genre_code: 'SCHOOL_ACTION_FANTASY'},
        {genre_code: 'COMIC_EVERYDAY_LIFE'},
        {genre_code: 'ROMANCE'},
        {genre_code: 'HORROR_THRILLER'},
        {genre_code: 'ROMANCE_FANTASY'},
        {genre_code: 'FANTASY_DRAMA'}
    ]
}

const summary = {
    Items: [
        {
            content_id: '24',
            genre_code: 'DRAMA',
            open_uu: '25750',
            open_cnt: '7328',
            total_gmv: '0'
        },
        {
            content_id: '44',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '36952',
            open_cnt: '7122',
            total_gmv: '2'
        },
        {
            content_id: '55',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '24556',
            open_cnt: '7494',
            total_gmv: '2'
        },
        {
            content_id: '56',
            genre_code: 'ROMANCE',
            open_uu: '39268',
            open_cnt: '6580',
            total_gmv: '2'
        },
        {
            content_id: '61',
            genre_code: 'HORROR_THRILLER',
            open_uu: '38944',
            open_cnt: '7262',
            total_gmv: '2'
        },
        {
            content_id: '64',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '29878',
            open_cnt: '7526',
            total_gmv: '2'
        },
        {
            content_id: '71',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '29128',
            open_cnt: '6330',
            total_gmv: '2'
        },
        {
            content_id: '78',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '28884',
            open_cnt: '6574',
            total_gmv: '0'
        },
        {
            content_id: '79',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '29790',
            open_cnt: '6392',
            total_gmv: '0'
        },
        {
            content_id: '22',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '25800',
            open_cnt: '7492',
            total_gmv: '4'
        },
        {
            content_id: '25',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '36970',
            open_cnt: '6712',
            total_gmv: '0'
        },
        {
            content_id: '32',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '35030',
            open_cnt: '7326',
            total_gmv: '0'
        },
        {
            content_id: '35',
            genre_code: 'ROMANCE',
            open_uu: '35476',
            open_cnt: '6350',
            total_gmv: '4'
        },
        {
            content_id: '45',
            genre_code: 'DRAMA',
            open_uu: '26064',
            open_cnt: '6184',
            total_gmv: '2'
        },
        {
            content_id: '48',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '21586',
            open_cnt: '7558',
            total_gmv: '0'
        },
        {
            content_id: '49',
            genre_code: 'ROMANCE',
            open_uu: '28534',
            open_cnt: '6314',
            total_gmv: '0'
        },
        {
            content_id: '52',
            genre_code: 'DRAMA',
            open_uu: '34448',
            open_cnt: '6048',
            total_gmv: '2'
        },
        {
            content_id: '54',
            genre_code: 'HORROR_THRILLER',
            open_uu: '25492',
            open_cnt: '7928',
            total_gmv: '4'
        },
        {
            content_id: '62',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '38728',
            open_cnt: '6466',
            total_gmv: '2'
        },
        {
            content_id: '67',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '24484',
            open_cnt: '7186',
            total_gmv: '2'
        },
        {
            content_id: '81',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '38202',
            open_cnt: '7742',
            total_gmv: '2'
        },
        {
            content_id: '82',
            genre_code: 'HORROR_THRILLER',
            open_uu: '34886',
            open_cnt: '6050',
            total_gmv: '0'
        },
        {
            content_id: '85',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '37822',
            open_cnt: '6568',
            total_gmv: '2'
        },
        {
            content_id: '12',
            genre_code: 'HORROR_THRILLER',
            open_uu: '20910',
            open_cnt: '6666',
            total_gmv: '2'
        },
        {
            content_id: '41',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '21340',
            open_cnt: '6482',
            total_gmv: '2'
        },
        {
            content_id: '42',
            genre_code: 'ROMANCE',
            open_uu: '30726',
            open_cnt: '6994',
            total_gmv: '4'
        },
        {
            content_id: '43',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '27634',
            open_cnt: '6810',
            total_gmv: '2'
        },
        {
            content_id: '47',
            genre_code: 'HORROR_THRILLER',
            open_uu: '34848',
            open_cnt: '6522',
            total_gmv: '0'
        },
        {
            content_id: '60',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '34818',
            open_cnt: '7602',
            total_gmv: '4'
        },
        {
            content_id: '70',
            genre_code: 'ROMANCE',
            open_uu: '26458',
            open_cnt: '6498',
            total_gmv: '2'
        },
        {
            content_id: '87',
            genre_code: 'DRAMA',
            open_uu: '25850',
            open_cnt: '7760',
            total_gmv: '2'
        },
        {
            content_id: '14',
            genre_code: 'ROMANCE',
            open_uu: '39750',
            open_cnt: '6638',
            total_gmv: '4'
        },
        {
            content_id: '29',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '24426',
            open_cnt: '6704',
            total_gmv: '4'
        },
        {
            content_id: '38',
            genre_code: 'DRAMA',
            open_uu: '25180',
            open_cnt: '6818',
            total_gmv: '0'
        },
        {
            content_id: '58',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '27562',
            open_cnt: '7760',
            total_gmv: '2'
        },
        {
            content_id: '84',
            genre_code: 'ROMANCE',
            open_uu: '26116',
            open_cnt: '6660',
            total_gmv: '4'
        },
        {
            content_id: '16',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '34548',
            open_cnt: '6746',
            total_gmv: '2'
        },
        {
            content_id: '17',
            genre_code: 'DRAMA',
            open_uu: '27460',
            open_cnt: '6254',
            total_gmv: '0'
        },
        {
            content_id: '21',
            genre_code: 'ROMANCE',
            open_uu: '34442',
            open_cnt: '6154',
            total_gmv: '4'
        },
        {
            content_id: '26',
            genre_code: 'HORROR_THRILLER',
            open_uu: '27562',
            open_cnt: '7696',
            total_gmv: '2'
        },
        {
            content_id: '40',
            genre_code: 'HORROR_THRILLER',
            open_uu: '37408',
            open_cnt: '7732',
            total_gmv: '4'
        },
        {
            content_id: '53',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '22754',
            open_cnt: '7330',
            total_gmv: '0'
        },
        {
            content_id: '75',
            genre_code: 'HORROR_THRILLER',
            open_uu: '37560',
            open_cnt: '7708',
            total_gmv: '4'
        },
        {
            content_id: '88',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '21314',
            open_cnt: '6722',
            total_gmv: '4'
        },
        {
            content_id: '20',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '27730',
            open_cnt: '7356',
            total_gmv: '0'
        },
        {
            content_id: '28',
            genre_code: 'ROMANCE',
            open_uu: '24302',
            open_cnt: '7282',
            total_gmv: '2'
        },
        {
            content_id: '30',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '27528',
            open_cnt: '6912',
            total_gmv: '2'
        },
        {
            content_id: '31',
            genre_code: 'DRAMA',
            open_uu: '24748',
            open_cnt: '6172',
            total_gmv: '2'
        },
        {
            content_id: '37',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '21830',
            open_cnt: '7102',
            total_gmv: '2'
        },
        {
            content_id: '50',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '28214',
            open_cnt: '6690',
            total_gmv: '2'
        },
        {
            content_id: '57',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '31398',
            open_cnt: '6312',
            total_gmv: '4'
        },
        {
            content_id: '69',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '26168',
            open_cnt: '6560',
            total_gmv: '0'
        },
        {
            content_id: '76',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '38752',
            open_cnt: '6924',
            total_gmv: '0'
        },
        {
            content_id: '77',
            genre_code: 'ROMANCE',
            open_uu: '27826',
            open_cnt: '6022',
            total_gmv: '2'
        },
        {
            content_id: '83',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '21240',
            open_cnt: '7504',
            total_gmv: '4'
        },
        {
            content_id: '86',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '38782',
            open_cnt: '6542',
            total_gmv: '0'
        },
        {
            content_id: '15',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '25236',
            open_cnt: '6252',
            total_gmv: '4'
        },
        {
            content_id: '18',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '24190',
            open_cnt: '6086',
            total_gmv: '0'
        },
        {
            content_id: '23',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '26298',
            open_cnt: '7034',
            total_gmv: '0'
        },
        {
            content_id: '27',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '37334',
            open_cnt: '7086',
            total_gmv: '2'
        },
        {
            content_id: '33',
            genre_code: 'HORROR_THRILLER',
            open_uu: '21642',
            open_cnt: '6258',
            total_gmv: '4'
        },
        {
            content_id: '34',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '21578',
            open_cnt: '6190',
            total_gmv: '2'
        },
        {
            content_id: '36',
            genre_code: 'ROMANCE_FANTASY',
            open_uu: '23956',
            open_cnt: '6146',
            total_gmv: '0'
        },
        {
            content_id: '46',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '39202',
            open_cnt: '6056',
            total_gmv: '2'
        },
        {
            content_id: '59',
            genre_code: 'DRAMA',
            open_uu: '22940',
            open_cnt: '6166',
            total_gmv: '2'
        },
        {
            content_id: '66',
            genre_code: 'DRAMA',
            open_uu: '26236',
            open_cnt: '6880',
            total_gmv: '0'
        },
        {
            content_id: '68',
            genre_code: 'HORROR_THRILLER',
            open_uu: '20548',
            open_cnt: '6070',
            total_gmv: '4'
        },
        {
            content_id: '72',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '28406',
            open_cnt: '6396',
            total_gmv: '0'
        },
        {
            content_id: '74',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '38200',
            open_cnt: '7228',
            total_gmv: '0'
        },
        {
            content_id: '80',
            genre_code: 'DRAMA',
            open_uu: '32934',
            open_cnt: '7142',
            total_gmv: '4'
        },
        {
            content_id: '13',
            genre_code: 'COMIC_EVERYDAY_LIFE',
            open_uu: '25536',
            open_cnt: '7528',
            total_gmv: '2'
        },
        {
            content_id: '19',
            genre_code: 'HORROR_THRILLER',
            open_uu: '25734',
            open_cnt: '6274',
            total_gmv: '0'
        },
        {
            content_id: '39',
            genre_code: 'FANTASY_DRAMA',
            open_uu: '37742',
            open_cnt: '7110',
            total_gmv: '0'
        },
        {
            content_id: '51',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '38470',
            open_cnt: '7368',
            total_gmv: '4'
        },
        {
            content_id: '63',
            genre_code: 'ROMANCE',
            open_uu: '20798',
            open_cnt: '7486',
            total_gmv: '4'
        },
        {
            content_id: '65',
            genre_code: 'SCHOOL_ACTION_FANTASY',
            open_uu: '29142',
            open_cnt: '6088',
            total_gmv: '2'
        },
        {
            content_id: '73',
            genre_code: 'DRAMA',
            open_uu: '24140',
            open_cnt: '7712',
            total_gmv: '2'
        },
        {
            content_id: '89',
            genre_code: 'HORROR_THRILLER',
            open_uu: '23624',
            open_cnt: '7594',
            total_gmv: '2'
        }
    ]
};

const targetDate = "2020-02-01-01";
const region = "ko";
const language = "kor";

describe('실시간 랭킹 생성 람다 함수 유닛 테스트', () => {

    it("통계데이터 스코어별 정렬 기능이 정상적으로 동작한다.", async () => {
        let sortedSummary = lambda.sort('open_uu', summary.Items);
        let beforeScore = Number(sortedSummary[0].open_uu);
        for (let i = 1; i < sortedSummary.length; i++) {
            expect(beforeScore).to.greaterThanOrEqual(Number(sortedSummary[i].open_uu));
            beforeScore = Number(sortedSummary[i].open_uu);
        }
        sortedSummary = lambda.sort('open_cnt', summary.Items);
        beforeScore = Number(sortedSummary[0].open_cnt);
        for (let i = 1; i < sortedSummary.length; i++) {
            expect(beforeScore).to.greaterThanOrEqual(Number(sortedSummary[i].open_cnt));
            beforeScore = Number(sortedSummary[i].open_uu);
        }
        sortedSummary = lambda.sort('total_gmv', summary.Items);
        beforeScore = Number(sortedSummary[0].total_gmv);
        for (let i = 1; i < sortedSummary.length; i++) {
            expect(beforeScore).to.greaterThanOrEqual(Number(sortedSummary[i].total_gmv));
            beforeScore = Number(sortedSummary[i].total_gmv);
        }
    });

    it("스코어별 순위에 가중치를 적용하는 기능이 정상적으로 동작한다.", async () => {
        let scoreBoard = new HashMap();
        let sortedSummary = lambda.sort("open_uu", summary.Items);
        const weight = WEIGHTS["kor"];
        sortedSummary.forEach(function (item, index) {
            if (item["open_uu"] <= 0) {
                lambda.caculateOverallScore(scoreBoard, item.content_id, 0);
            } else {
                lambda.caculateOverallScore(scoreBoard, item.content_id, (sortedSummary.length - index) * weight[0]);
            }
        });
        sortedSummary.forEach(function (item, index) {
            expect(scoreBoard.get(item.content_id)).to.be.eq((sortedSummary.length - index) * weight[0]);
        });

    });

    it("통합 점수를 기준으로 정렬한 데이터와 전체 랭킹을 산정하는 함수의 결과가 동일하다", async () => {
        process.env.LOCALE = language;
        const weight = WEIGHTS[language];
        const types = ["open_uu", "open_cnt", "total_gmv"];
        let scoreBoard = new HashMap();
        let sortedSummary;
        // 타입별로 작품의 점수를 산정하여 합산한다.
        types.forEach(function (type, index) {
            sortedSummary = lambda.sort(type, summary.Items);
            sortedSummary.forEach(function (item, index) {
                if (item[type] <= 0) {
                    lambda.caculateOverallScore(scoreBoard, item.content_id, 0);
                } else {
                    lambda.caculateOverallScore(scoreBoard, item.content_id, (sortedSummary.length - index) * weight[0]);
                }
            });
        });
        // 통합 점수를 total_score 필드에 기록한다
        sortedSummary.forEach(function (item) {
            item.total_score = scoreBoard.get(item.content_id);
        });
        // 통합 점수 기준으로 데이터를 정렬한다
        lambda.sort("total_score", sortedSummary);

        // 통합 점수를 기준으로 정렬한 데이터와 전체 랭킹을 산정하는 함수의 결과가 동일한지 확인
        sinon.stub(lambda, "getContentSummary").returns(summary);
        const totalRanking = await lambda.makeTotalRanking(targetDate, region, language);
        sortedSummary.forEach(function (item, index) {
            expect(item.total_score).to.be.eq(totalRanking[index].total_score);
        });
    });

    it("전체 랭킹 기반으로 장르별 랭킹이 정상적으로 생성된다.", async () => {
        const totalRanking = await lambda.makeTotalRanking(targetDate, region, language);
        const rankingMap = lambda.makeGenreRanking(totalRanking, genres);
        expect(rankingMap.size).to.be.eq(8);
        for (let key of rankingMap.keys()) {
            expect(rankingMap.get(key).length).to.be.greaterThan(0);
        }
    });

    it("랭킹정보에 등락정보가 올바로 생성된다.", async () => {
        const totalRanking = await lambda.makeTotalRanking(targetDate, region, language);
        let rankingMap = lambda.makeGenreRanking(totalRanking, genres);
        const lastRanking = new HashMap();
        lastRanking.set(54, 10);
        lastRanking.set(40, 18);
        lastRanking.set(75, 45);
        lastRanking.set(60, 28);
        lastRanking.set(83, 5);
        lastRanking.set(22, 58);
        lastRanking.set(63, 32);
        lastRanking.set(51, 7);
        lastRanking.set(80, 70);
        lastRanking.set(42, 25);
        sinon.stub(lambda, "getBeforeRankingDt").returns({Items: [{target_dt: targetDate}]});
        sinon.stub(lambda, "getRankingChart").returns(lastRanking);
        rankingMap = await lambda.setRankAndUpDownInfo(targetDate, region, language, rankingMap);
        const rankings = rankingMap.get("ALL");
        for (let ranking of rankings) {
            if (lastRanking.has(ranking.content_id)) {
                expect(ranking.rank_last).to.be.eq(lastRanking.get(ranking.content_id));
            }
        }
    });

    it("랭킹정보로 올바른 포맷의 이벤트 메시지가 생성된다", async () => {
        const genreCode = "DRAMA"
        const totalRanking = await lambda.makeTotalRanking(targetDate, region, language);
        let rankingMap = lambda.makeGenreRanking(totalRanking, genres);
        rankingMap = await lambda.setRankAndUpDownInfo(targetDate, region, language, rankingMap);
        const domainEvent = JSON.parse(await lambda.makeDomainEvent(region, language, genreCode, JSON.parse(rankingMap.get(genreCode))));
        expect(domainEvent.event.type).to.be.eq("CREATE");
        expect(domainEvent.event.attributes[0].region).to.be.eq(region);
        expect(domainEvent.event.attributes[0].language).to.be.eq(language);
        expect(domainEvent.event.attributes[0].list.length).to.be.eq(1);
        expect(domainEvent.event.attributes[0].list[0].type).to.be.eq("HOURLY");
        expect(domainEvent.event.attributes[0].list[0].genre_code).to.be.eq(genreCode);
        expect(domainEvent.event.attributes[0].list[0].ranking.title).to.be.eq("HOURLY RANKING");
        expect(domainEvent.event.attributes[0].list[0].ranking.total).to.be.eq(11);
        expect(domainEvent.event.attributes[0].list[0].ranking.list.length).to.be.eq(11);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[0].content_id).to.be.eq(80);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[0].rank).to.be.eq(1);
        expect(domainEvent.event.attributes[0].list[0].ranking.list[0].rank_last).to.be.eq(70);
    });
});

